// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT

package controlplaneaggregator

import (
    "context"
    "go.opentelemetry.io/collector/pdata/pmetric"
    "go.opentelemetry.io/collector/pdata/pcommon"
    "go.opentelemetry.io/collector/consumer"
    "go.opentelemetry.io/collector/component"
    "go.uber.org/zap"
    // "fmt"
    // "strings"
    "math"
    "strings"
    "time"
    "sync"
)

type controlPlaneAggregatorProcessor struct {
    logger *zap.Logger
    buffer map[string]map[string]*metricValues
    mu sync.RWMutex
    stopChan chan struct{}
    nextConsumer consumer.Metrics
}

// metricValues holds the values for a specific metric name and attribute combination
type metricValues struct {
    histograms []pmetric.HistogramDataPoint
    gauges     []pmetric.NumberDataPoint
    sums       []pmetric.NumberDataPoint
}

func (p *controlPlaneAggregatorProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
    p.mu.Lock()
    defer p.mu.Unlock()
    
    p.bufferMetrics(md)
    return nil
}

func (p *controlPlaneAggregatorProcessor) Capabilities() consumer.Capabilities {
    return consumer.Capabilities{MutatesData: true}
}

func newControlPlaneAggregatorProcessor(logger *zap.Logger, nextConsumer consumer.Metrics) *controlPlaneAggregatorProcessor {
    return &controlPlaneAggregatorProcessor{
        logger: logger,
        buffer: make(map[string]map[string]*metricValues),
        stopChan: make(chan struct{}),
        nextConsumer: nextConsumer,
    }
}

func (p *controlPlaneAggregatorProcessor) Start(ctx context.Context, host component.Host) error {
    go p.startPreciseTicker()
    return nil
}

func (p *controlPlaneAggregatorProcessor) Shutdown(ctx context.Context) error {
    close(p.stopChan)
    p.flushBuffer(ctx)
    return nil
}

func (p *controlPlaneAggregatorProcessor) startPreciseTicker() {
    
    for {
        // Calculate next minute boundary every time
        now := time.Now()
        nextMinute := now.Truncate(time.Minute).Add(time.Minute)
        
        // Sleep until that exact time
        time.Sleep(time.Until(nextMinute))
        
        // Flush at exactly :00
        p.flushBuffer(context.Background())
        
        // Check for shutdown
        select {
        case <-p.stopChan:
            return
        default:
            // Continue to next iteration
        }
    }
    
}


// New method to buffer incoming metrics
func (p *controlPlaneAggregatorProcessor) bufferMetrics(md pmetric.Metrics) {
    for i := 0; i < md.ResourceMetrics().Len(); i++ {
        rm := md.ResourceMetrics().At(i)
        // serviceName, exists := rm.Resource().Attributes().Get("service.name")
        
        // if !exists || !strings.Contains(serviceName.AsString(), "containerInsightsKubeAPIServerScraper") {
        //     continue
        // }
        
        for j := 0; j < rm.ScopeMetrics().Len(); j++ {
            sm := rm.ScopeMetrics().At(j)
            metricGroups := p.matchMetrics(sm)
            
            for metricName, attrMap := range metricGroups {
                if p.buffer[metricName] == nil {
                    p.buffer[metricName] = make(map[string]*metricValues)
                }
                
                for attrHash, values := range attrMap {
                    if p.buffer[metricName][attrHash] == nil {
                        p.buffer[metricName][attrHash] = &metricValues{}
                    }
                    
                    existing := p.buffer[metricName][attrHash]
                    existing.histograms = append(existing.histograms, values.histograms...)
                    existing.gauges = append(existing.gauges, values.gauges...)
                    existing.sums = append(existing.sums, values.sums...)
                }
            }
        }
    }
}

// Flush method called by ticker
func (p *controlPlaneAggregatorProcessor) flushBuffer(ctx context.Context) {
    p.mu.Lock()
    bufferedMetrics := p.buffer
    p.buffer = make(map[string]map[string]*metricValues)
    p.mu.Unlock()
    
    if len(bufferedMetrics) == 0 {
        return
    }
    
    newMetrics := pmetric.NewMetrics()
    newRM := newMetrics.ResourceMetrics().AppendEmpty()
    newSM := newRM.ScopeMetrics().AppendEmpty()
    
    p.addMetricsToRMS(bufferedMetrics, newSM)
    
    if newSM.Metrics().Len() > 0 {
        p.nextConsumer.ConsumeMetrics(ctx, newMetrics)
    }
}

// matchMetrics groups metrics by name and attributes
func (p *controlPlaneAggregatorProcessor) matchMetrics(sm pmetric.ScopeMetrics) map[string]map[string]*metricValues {
    // Map of metric name -> attribute hash -> metric values
    metricGroups := make(map[string]map[string]*metricValues)
    
    // Process each metric
    for i := 0; i < sm.Metrics().Len(); i++ {
        metric := sm.Metrics().At(i)
        metricName := metric.Name()
        
        // Initialize the map for this metric name if it doesn't exist
        if _, ok := metricGroups[metricName]; !ok {
            metricGroups[metricName] = make(map[string]*metricValues)
        }
        
        // Process based on data type
        switch metric.Type() {
        case pmetric.MetricTypeHistogram:
            p.processHistogram(metric, metricGroups[metricName])
        case pmetric.MetricTypeGauge:
            p.processGauge(metric, metricGroups[metricName])
        case pmetric.MetricTypeSum:
            p.processSum(metric, metricGroups[metricName])
        }
    }
    
    return metricGroups
}

// getAttributeHashWithMinute generates a string key from attributes and timestamp minute for grouping
func getAttributeHash(attrs pcommon.Map) string {
    // Simple implementation - in production you might want a more efficient hash
    var key strings.Builder
    attrs.Range(func(k string, v pcommon.Value) bool {
        key.WriteString(k)
        key.WriteString("=")
        key.WriteString(v.AsString())
        key.WriteString(";")
        return true
    })
    return key.String()
}

// processHistogram processes a histogram metric
func (p *controlPlaneAggregatorProcessor) processHistogram(metric pmetric.Metric, attrMap map[string]*metricValues) {
    hist := metric.Histogram()
    
    for i := 0; i < hist.DataPoints().Len(); i++ {
        dp := hist.DataPoints().At(i)
        attrHash := getAttributeHash(dp.Attributes())
        
        if _, ok := attrMap[attrHash]; !ok {
            attrMap[attrHash] = &metricValues{}
        }
        
        // Store the histogram data point
        attrMap[attrHash].histograms = append(attrMap[attrHash].histograms, dp)
    }
}

// processGauge processes a gauge metric
func (p *controlPlaneAggregatorProcessor) processGauge(metric pmetric.Metric, attrMap map[string]*metricValues) {
    gauge := metric.Gauge()
    
    for i := 0; i < gauge.DataPoints().Len(); i++ {
        dp := gauge.DataPoints().At(i)
        attrHash := getAttributeHash(dp.Attributes())
        
        if _, ok := attrMap[attrHash]; !ok {
            attrMap[attrHash] = &metricValues{}
        }
        
        // Store the gauge data point
        attrMap[attrHash].gauges = append(attrMap[attrHash].gauges, dp)
    }
}

// processSum processes a sum metric
func (p *controlPlaneAggregatorProcessor) processSum(metric pmetric.Metric, attrMap map[string]*metricValues) {
    sum := metric.Sum()
    
    for i := 0; i < sum.DataPoints().Len(); i++ {
        dp := sum.DataPoints().At(i)
        attrHash := getAttributeHash(dp.Attributes())
        
        if _, ok := attrMap[attrHash]; !ok {
            attrMap[attrHash] = &metricValues{}
        }
        
        // Store the sum data point
        attrMap[attrHash].sums = append(attrMap[attrHash].sums, dp)
    }
}

// addMetricsToRMS adds the aggregated metrics to the scope metrics
func (p *controlPlaneAggregatorProcessor) addMetricsToRMS(metricGroups map[string]map[string]*metricValues, newSM pmetric.ScopeMetrics) {
    // Process each metric name
    for metricName, attrMap := range metricGroups {
        // Process each attribute combination
        for _, values := range attrMap {
            // Process histograms
            if len(values.histograms) > 0 {
                p.aggregateHistograms(metricName, values.histograms, newSM)
            }
            
            // Process gauges
            if len(values.gauges) > 0 {
                p.aggregateGaugesToExponentialHistogram(metricName, values.gauges, newSM)
            }
            
            // Process sums
            if len(values.sums) > 0 {
                p.aggregateSumsToExponentialHistogram(metricName, values.sums, newSM)
            }
        }
    }
}

// aggregateHistograms aggregates histograms with the same attributes
func (p *controlPlaneAggregatorProcessor) aggregateHistograms(
    metricName string, 
    histograms []pmetric.HistogramDataPoint, 
    newSM pmetric.ScopeMetrics) {
    
    if len(histograms) == 0 {
        return
    }
    
    // Create a new metric
    newMetric := newSM.Metrics().AppendEmpty()
    newMetric.SetName(metricName)
    
    // Create a new histogram
    newHist := newMetric.SetEmptyHistogram()

    newHist.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
    
    // Create a new data point
    newDP := newHist.DataPoints().AppendEmpty()
    
    // Copy attributes from the first data point
    histograms[0].Attributes().CopyTo(newDP.Attributes())
    
    // Set start and end time from the first data point
    newDP.SetStartTimestamp(histograms[0].StartTimestamp())
    newDP.SetTimestamp(histograms[0].Timestamp())
    
    // Initialize aggregated values
    newDP.SetCount(0)
    newDP.SetSum(0)
    
    // Copy explicit bounds from the first data point
    explicitBounds := histograms[0].ExplicitBounds()
    newDP.ExplicitBounds().FromRaw(explicitBounds.AsRaw())
    
    // Initialize bucket counts with zeros
    newDP.BucketCounts().FromRaw(make([]uint64, len(explicitBounds.AsRaw())+1))
    
    // Aggregate data from all histograms
    for _, dp := range histograms {
        // Add count and sum
        newDP.SetCount(newDP.Count() + dp.Count())
        newDP.SetSum(newDP.Sum() + dp.Sum())
        
        // Add bucket counts
        for j := 0; j < dp.BucketCounts().Len(); j++ {
            newDP.BucketCounts().SetAt(j, newDP.BucketCounts().At(j) + dp.BucketCounts().At(j))
        }
        
        // Update min/max if available
        if dp.HasMin() {
            if !newDP.HasMin() || dp.Min() < newDP.Min() {
                newDP.SetMin(dp.Min())
            }
        }
        if dp.HasMax() {
            if !newDP.HasMax() || dp.Max() > newDP.Max() {
                newDP.SetMax(dp.Max())
            }
        }
    }
}

// aggregateGaugesToExponentialHistogram aggregates gauges to an exponential histogram
func (p *controlPlaneAggregatorProcessor) aggregateGaugesToExponentialHistogram(
    metricName string, 
    gauges []pmetric.NumberDataPoint, 
    newSM pmetric.ScopeMetrics) {
    
    if len(gauges) == 0 {
        return
    }
    
    // Create a new metric
    newMetric := newSM.Metrics().AppendEmpty()
    newMetric.SetName(metricName)
    
    // Create a new exponential histogram
    newExpHist := newMetric.SetEmptyExponentialHistogram()
    newExpHist.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
    
    // Create a new data point
    newDP := newExpHist.DataPoints().AppendEmpty()
    
    // Copy attributes from the first data point
    gauges[0].Attributes().CopyTo(newDP.Attributes())
    
    newDP.SetStartTimestamp(gauges[0].StartTimestamp())
    newDP.SetTimestamp(gauges[0].Timestamp())
    
    // Set scale for the exponential histogram (can be adjusted based on data range)
    newDP.SetScale(0) // Default scale, can be adjusted dynamically if needed
    
    // Initialize min, max, sum, and count
    var min, max, sum float64
    var count uint64
    hasMin := false
    
    // Collect all gauge values and add to buckets
    for _, dp := range gauges {
        val := dp.DoubleValue()
        
        // Update min/max/sum
        if !hasMin || val < min {
            min = val
            hasMin = true
        }
        if !hasMin || val > max {
            max = val
        }
        sum += val
        count++
        
        // Add to appropriate bucket
        p.addValueToBucket(val, newDP)
    }
    
    // Set min, max, sum, and count
    newDP.SetMin(min)
    newDP.SetMax(max)
    newDP.SetSum(sum)
    newDP.SetCount(count)
    
}

// aggregateSumsToExponentialHistogram aggregates sums to an exponential histogram
// Implementation is similar to aggregateGaugesToExponentialHistogram
func (p *controlPlaneAggregatorProcessor) aggregateSumsToExponentialHistogram(
    metricName string, 
    sums []pmetric.NumberDataPoint, 
    newSM pmetric.ScopeMetrics) {
    
    // Implementation is almost identical to aggregateGaugesToExponentialHistogram
    // Reuse the same logic for simplicity
    p.aggregateGaugesToExponentialHistogram(metricName, sums, newSM)
}

// addValueToBucket adds a value to the appropriate bucket in an exponential histogram
func (p *controlPlaneAggregatorProcessor) addValueToBucket(val float64, dp pmetric.ExponentialHistogramDataPoint) {
    if val == 0 {
        // Zero values are tracked separately
        dp.SetZeroCount(dp.ZeroCount() + 1)
        return
    }
    
    // Determine bucket index based on the value and scale
    bucketIndex := p.determineBucketIndex(val, dp.Scale())
    
    // Add to positive buckets
    for dp.Positive().BucketCounts().Len() <= bucketIndex {
        dp.Positive().BucketCounts().Append(0)
    }
    dp.Positive().BucketCounts().SetAt(bucketIndex, 
        dp.Positive().BucketCounts().At(bucketIndex) + 1)
}

// determineBucketIndex calculates the bucket index for a value in an exponential histogram
func (p *controlPlaneAggregatorProcessor) determineBucketIndex(value float64, scale int32) int {
    // This is a simplified implementation
    // In a real implementation, you would use the scale to determine the bucket
    // based on the exponential histogram algorithm
    
    absValue := math.Abs(value)
    
    // Simple logarithmic bucketing based on scale
    // The actual implementation would depend on the OpenTelemetry spec for exponential histograms
    base := math.Pow(2, float64(scale))
    
    if absValue < 1 {
        return 0
    }
    
    // Calculate bucket index based on logarithm
    return int(math.Log2(absValue) * base)
}

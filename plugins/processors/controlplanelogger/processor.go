// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT

package controlplanelogger

import (
    "context"
    "go.opentelemetry.io/collector/pdata/pmetric"
    "go.uber.org/zap"
    "fmt"
    "strings"
    "time"
)

type controlPlaneLoggerProcessor struct {
    logger *zap.Logger
}

func newControlPlaneLoggerProcessor(logger *zap.Logger) *controlPlaneLoggerProcessor {
    return &controlPlaneLoggerProcessor{
        logger: logger,
    }
}

func (p *controlPlaneLoggerProcessor) processMetrics(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
    // Log all metrics to identify control plane patterns
    // p.logMd(md, "ALL_METRICS")
    // p.logger.Info("Agent running")
    // p.logMetricCounts(md)
    p.logger.Info("Logging time", zap.Time("timestamp", time.Now()))
    return md, nil
}

func (p *controlPlaneLoggerProcessor) logMd(md pmetric.Metrics, name string) {
    var logMessage strings.Builder

    logMessage.WriteString(fmt.Sprintf("\"%s_METRICS_MD\" : {\n", name))
    rms := md.ResourceMetrics()
    for i := 0; i < rms.Len(); i++ {
        rs := rms.At(i)
        rs.Resource().Attributes().AsRaw()
        ilms := rs.ScopeMetrics()
        logMessage.WriteString(fmt.Sprintf("\t\"ResourceMetric_%d\": {\n", i))
        logMessage.WriteString(fmt.Sprintf("\t\t\"Resource attributes\": %s,\n", rs.Resource().Attributes().AsRaw()))
        for j := 0; j < ilms.Len(); j++ {
            ils := ilms.At(j)
            metrics := ils.Metrics()
            logMessage.WriteString(fmt.Sprintf("\t\t\"ScopeMetric_%d\": {\n", j))
            logMessage.WriteString(fmt.Sprintf("\t\t\"Metrics_%d\": [\n", j))

            for k := 0; k < metrics.Len(); k++ {
                m := metrics.At(k)
                logMessage.WriteString(fmt.Sprintf("\t\t\t\"Metric_%d\": {\n", k))
                logMessage.WriteString(fmt.Sprintf("\t\t\t\t\"name\": \"%s\",\n", m.Name()))
                logMessage.WriteString(fmt.Sprintf("\t\t\t\t\"type\": \"%s\",\n", m.Type()))

                switch m.Type() {
                case pmetric.MetricTypeGauge:
                    datapoints := m.Gauge().DataPoints()
                    for yu := 0; yu < datapoints.Len(); yu++ {
                        logMessage.WriteString("\t\t\t\t\t{\n")
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"attributes\": \"%v\",\n", datapoints.At(yu).Attributes().AsRaw()))
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"value\": %v,\n", datapoints.At(yu).DoubleValue()))
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"timestamp\": %v,\n", datapoints.At(yu).Timestamp()))
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"flags\": %v,\n", datapoints.At(yu).Flags()))
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"value type\": %v,\n", datapoints.At(yu).ValueType()))
                        logMessage.WriteString("\t\t\t\t\t},\n")
                    }
                case pmetric.MetricTypeSum:
                    datapoints := m.Sum().DataPoints()
                    logMessage.WriteString(fmt.Sprintf("\t\t\t\t\"aggregation_temporality\": \"%s\",\n", m.Sum().AggregationTemporality().String()))
                    logMessage.WriteString(fmt.Sprintf("\t\t\t\t\"Monotonic: \": \"%t\",\n", m.Sum().IsMonotonic()))
                    for yu := 0; yu < datapoints.Len(); yu++ {
                        logMessage.WriteString("\t\t\t\t\t{\n")
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"attributes\": \"%v\",\n", datapoints.At(yu).Attributes().AsRaw()))
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"value\": %v,\n", datapoints.At(yu).DoubleValue()))
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"timestamp\": %v,\n", datapoints.At(yu).Timestamp()))
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"flags\": %v,\n", datapoints.At(yu).Flags()))
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"value type\": %v,\n", datapoints.At(yu).ValueType()))
                        logMessage.WriteString("\t\t\t\t\t},\n")
                    }
                case pmetric.MetricTypeExponentialHistogram:
                    datapoints := m.ExponentialHistogram().DataPoints()
                    logMessage.WriteString(fmt.Sprintf("\t\t\t\t\"aggregation_temporality\": \"%s\",\n", m.ExponentialHistogram().AggregationTemporality().String()))
                    for yu := 0; yu < datapoints.Len(); yu++ {
                        ehdp := datapoints.At(yu)
                        logMessage.WriteString("\t\t\t\t\t{\n")
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"attributes\": \"%v\",\n", ehdp.Attributes().AsRaw()))
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"count\": %v,\n", ehdp.Count()))
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"sum\": %v,\n", ehdp.Sum()))
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"scale\": %v,\n", ehdp.Scale()))
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"zero_count\": %v,\n", ehdp.ZeroCount()))
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"HasMin\": %t,\n", ehdp.HasMin()))
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"min\": %v,\n", ehdp.Min()))
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"HasMax\": %t,\n", ehdp.HasMax()))
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"max\": %v,\n", ehdp.Max()))
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"timestamp\": %v,\n", ehdp.Timestamp()))
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"starttimestamp\": %v,\n", ehdp.StartTimestamp()))

                        logMessage.WriteString("\t\t\t\t\t\t\"positive_buckets\": {\n")
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\t\"offset\": %v,\n", ehdp.Positive().Offset()))
                        logMessage.WriteString("\t\t\t\t\t\t\t\"counts\": [")
                        for b := 0; b < ehdp.Positive().BucketCounts().Len(); b++ {
                            logMessage.WriteString(fmt.Sprintf("%v", ehdp.Positive().BucketCounts().At(b)))
                            if b < ehdp.Positive().BucketCounts().Len()-1 {
                                logMessage.WriteString(", ")
                            }
                        }
                        logMessage.WriteString("]\n\t\t\t\t\t\t},\n")
                        logMessage.WriteString("\t\t\t\t\t},\n")
                        logMessage.WriteString("\t\t\t\t\t\t\"negative_buckets\": {\n")
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\t\"offset\": %v,\n", ehdp.Negative().Offset()))
                        logMessage.WriteString("\t\t\t\t\t\t\t\"counts\": [")
                        for b := 0; b < ehdp.Negative().BucketCounts().Len(); b++ {
                            logMessage.WriteString(fmt.Sprintf("%v", ehdp.Negative().BucketCounts().At(b)))
                            if b < ehdp.Negative().BucketCounts().Len()-1 {
                                logMessage.WriteString(", ")
                            }
                        }

                        logMessage.WriteString("]\n\t\t\t\t\t\t},\n")
                        logMessage.WriteString("\t\t\t\t\t},\n")
                    }
                case pmetric.MetricTypeHistogram:
                    histogramDatapoints := m.Histogram().DataPoints()
                   
                    logMessage.WriteString(fmt.Sprintf("\t\t\t\t\"aggregation_temporality\": \"%s\",\n", m.Histogram().AggregationTemporality().String()))
                    for yu := 0; yu < histogramDatapoints.Len(); yu++ {
                        hdp := histogramDatapoints.At(yu)
                        logMessage.WriteString("\t\t\t\t\t{\n")
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"attributes\": \"%v\",\n", hdp.Attributes().AsRaw()))
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"count\": %v,\n", hdp.Count()))
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"bounds\": %v,\n", hdp.ExplicitBounds()))
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"sum\": %v,\n", hdp.Sum()))
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"HasMin\": %t,\n", hdp.HasMin()))
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"min\": %v,\n", hdp.Min()))
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"HasMax\": %t,\n", hdp.HasMax()))
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"max\": %v,\n", hdp.Max()))
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"timestamp\": %v,\n", hdp.Timestamp()))
                        logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"starttimestamp\": %v,\n", hdp.StartTimestamp()))
                        
                        // Log bucket boundaries and counts
                        logMessage.WriteString("\t\t\t\t\t\t\"buckets\": [\n")
                        for b := 0; b < hdp.BucketCounts().Len(); b++ {
                            if b < hdp.ExplicitBounds().Len() {
                                logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\t{\"bound\": %v, \"count\": %v},\n", 
                                    hdp.ExplicitBounds().At(b), hdp.BucketCounts().At(b)))
                            } else {
                                logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\t{\"bound\": \"+Inf\", \"count\": %v},\n", 
                                    hdp.BucketCounts().At(b)))
                            }
                        }
                        logMessage.WriteString("\t\t\t\t\t\t],\n")
                        
                        logMessage.WriteString("\t\t\t\t\t},\n")
                    }
                    
                }
                logMessage.WriteString("\t\t\t\t],\n")
                logMessage.WriteString("\t\t\t},\n")
            }
            logMessage.WriteString("\t\t],\n")
            logMessage.WriteString("\t\t},\n")
        }
        logMessage.WriteString("\t},\n")
    }
    logMessage.WriteString("},\n")

    p.logger.Info(logMessage.String())
}

func (p *controlPlaneLoggerProcessor) logMetricCounts(md pmetric.Metrics) {
    // Create a fresh map each time
    metricCounts := make(map[string]int)
    
    // Count metrics in this batch only
    metricNames := make(map[string]bool)
    totalDataPoints := 0
    
    rms := md.ResourceMetrics()
    for i := 0; i < rms.Len(); i++ {
        rs := rms.At(i)
        ilms := rs.ScopeMetrics()
        
        for j := 0; j < ilms.Len(); j++ {
            ils := ilms.At(j)
            metrics := ils.Metrics()
            
            for k := 0; k < metrics.Len(); k++ {
                m := metrics.At(k)
                
                // Only track control plane metrics
                if !strings.HasPrefix(m.Name(), "apiserver_") && 
                   !strings.HasPrefix(m.Name(), "etcd_") && 
                   !strings.HasPrefix(m.Name(), "rest_client_") {
                    continue
                }
                
                metricNames[m.Name()] = true
                
                // Count datapoints
                switch m.Type() {
                case pmetric.MetricTypeGauge:
                    totalDataPoints += m.Gauge().DataPoints().Len()
                    countDataPoints(m.Name(), m.Gauge().DataPoints(), metricCounts)
                case pmetric.MetricTypeSum:
                    totalDataPoints += m.Sum().DataPoints().Len()
                    countDataPoints(m.Name(), m.Sum().DataPoints(), metricCounts)
                case pmetric.MetricTypeHistogram:
                    totalDataPoints += m.Histogram().DataPoints().Len()
                    countHistogramPoints(m.Name(), m.Histogram().DataPoints(), metricCounts)
                }

                // if m.Name() == "apiserver_request_duration_seconds" {
                //     totalDataPoints += m.Histogram().DataPoints().Len()
                //     countHistogramPoints(m.Name(), m.Histogram().DataPoints(), metricCounts)
                // }
            }
        }
    }
    
    // Log simple stats
    p.logger.Info(fmt.Sprintf("Batch stats: %d unique metric names, %d total datapoints, %d unique metric+dimension combinations", 
        len(metricNames), totalDataPoints, len(metricCounts)))
    
    // Use StringBuilder to log ALL metrics in one message
    var logMessage strings.Builder
    logMessage.WriteString("\nMETRICS FOUND AND MATCHED:\n\n")
    
    // Log ALL metrics
    for key, val := range metricCounts {
        logMessage.WriteString(fmt.Sprintf("Metric: %s\nCount: %d\n\n", key, val))
    }
    
    
    // Log everything in ONE call
    p.logger.Info(logMessage.String())
}



// Helper function to count datapoints
func countDataPoints(metricName string, dps pmetric.NumberDataPointSlice, counts map[string]int) {
    for i := 0; i < dps.Len(); i++ {
        dp := dps.At(i)
        attrs := dp.Attributes().AsRaw()
        
        // Remove node-specific attributes
        attrsCopy := make(map[string]interface{})
        for k, v := range attrs {
            if k != "kubernetes.io/hostname" && k != "host.name" {
                attrsCopy[k] = v
            }
        }
        
        // Create key from name + dimensions
        key := fmt.Sprintf("%s:%v", metricName, attrsCopy)
        counts[key]++
    }
}

// Helper function to count histogram datapoints
func countHistogramPoints(metricName string, dps pmetric.HistogramDataPointSlice, counts map[string]int) {
    for i := 0; i < dps.Len(); i++ {
        dp := dps.At(i)
        attrs := dp.Attributes().AsRaw()
        
        // Remove node-specific attributes
        attrsCopy := make(map[string]interface{})
        for k, v := range attrs {
            if k != "kubernetes.io/hostname" && k != "host.name" {
                attrsCopy[k] = v
            }
        }
        
        // Create key from name + dimensions
        key := fmt.Sprintf("%s:%v", metricName, attrsCopy)
        counts[key]++
    }
}

// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT

package groupbyattrsprocessor

import (
	"fmt"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/groupbyattrsprocessor"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/processor"

	"github.com/aws/amazon-cloudwatch-agent/translator/translate/otel/common"
)

type translator struct {
	name    string
	factory processor.Factory
}

var _ common.ComponentTranslator = (*translator)(nil)

func NewTranslatorWithName(name string) common.ComponentTranslator {
	return &translator{name, groupbyattrsprocessor.NewFactory()}
}

func (t *translator) ID() component.ID {
	return component.NewIDWithName(t.factory.Type(), t.name)
}

func (t *translator) Translate(conf *confmap.Conf) (component.Config, error) {
	cfg := t.factory.CreateDefaultConfig()
	
	c := confmap.NewFromStringMap(map[string]interface{}{
		"keys": []string{},
	})
	
	if err := c.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unable to unmarshal groupbyattrs processor (%s): %w", t.ID(), err)
	}
	
	return cfg, nil
}

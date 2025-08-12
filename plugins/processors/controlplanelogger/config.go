// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT

package controlplanelogger

import (
    "go.opentelemetry.io/collector/component"
)

// Config holds the configuration for the controlplanelogger processor.
type Config struct{}

// Verify Config implements component.Config interface.
var _ component.Config = (*Config)(nil)

// Validate validates the processor configuration.
func (cfg *Config) Validate() error {
    return nil
}
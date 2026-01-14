package flow

import (
	"testing"
	"time"
)

// TestAdaptiveThrottlerConfig_Validate tests configuration validation with valid and invalid settings.
func TestAdaptiveThrottlerConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  AdaptiveThrottlerConfig
		wantErr bool
	}{
		{
			name: "Valid Config",
			config: AdaptiveThrottlerConfig{
				MaxMemoryPercent: 80,
				MaxCPUPercent:    70,
				SampleInterval:   100 * time.Millisecond,
				CPUUsageMode:     CPUUsageModeMeasured,
				InitialRate:      100,
				MinRate:          10,
				MaxRate:          1000,
				BackoffFactor:    0.5,
				RecoveryFactor:   1.2,
			},
			wantErr: false,
		},
		{
			name: "Invalid SampleInterval",
			config: AdaptiveThrottlerConfig{
				SampleInterval: 1 * time.Millisecond,
			},
			wantErr: true,
		},
		{
			name: "Invalid MaxMemoryPercent High",
			config: AdaptiveThrottlerConfig{
				SampleInterval:   100 * time.Millisecond,
				MaxMemoryPercent: 101,
			},
			wantErr: true,
		},
		{
			name: "Invalid BackoffFactor",
			config: AdaptiveThrottlerConfig{
				SampleInterval:   100 * time.Millisecond,
				MaxMemoryPercent: 80,
				BackoffFactor:    1.5,
			},
			wantErr: true,
		},
		{
			name: "Invalid InitialRate below MinRate",
			config: AdaptiveThrottlerConfig{
				SampleInterval:   100 * time.Millisecond,
				MaxMemoryPercent: 80,
				MinRate:          10,
				MaxRate:          100,
				InitialRate:      5,
				BackoffFactor:    0.5,
				RecoveryFactor:   1.2,
			},
			wantErr: true,
		},
		{
			name: "Invalid RecoveryFactor too low",
			config: AdaptiveThrottlerConfig{
				SampleInterval:   100 * time.Millisecond,
				MaxMemoryPercent: 80,
				MinRate:          10,
				MaxRate:          100,
				InitialRate:      50,
				BackoffFactor:    0.5,
				RecoveryFactor:   0.9,
			},
			wantErr: true,
		},
		{
			name: "Valid InitialRate equals MinRate",
			config: AdaptiveThrottlerConfig{
				SampleInterval:   100 * time.Millisecond,
				MaxMemoryPercent: 80,
				MinRate:          10,
				MaxRate:          100,
				InitialRate:      10,
				BackoffFactor:    0.5,
				RecoveryFactor:   1.2,
			},
			wantErr: false,
		},
		{
			name: "Valid InitialRate equals MaxRate",
			config: AdaptiveThrottlerConfig{
				SampleInterval:   100 * time.Millisecond,
				MaxMemoryPercent: 80,
				MinRate:          10,
				MaxRate:          100,
				InitialRate:      100,
				BackoffFactor:    0.5,
				RecoveryFactor:   1.2,
			},
			wantErr: false,
		},
		{
			name: "Invalid RecoveryMemoryThreshold too high",
			config: AdaptiveThrottlerConfig{
				SampleInterval:          100 * time.Millisecond,
				MaxMemoryPercent:        80,
				RecoveryMemoryThreshold: 85, // Higher than MaxMemoryPercent
				MinRate:                 10,
				MaxRate:                 100,
				InitialRate:             50,
				BackoffFactor:           0.5,
				RecoveryFactor:          1.2,
			},
			wantErr: true,
		},
		{
			name: "Invalid RecoveryCPUThreshold too high",
			config: AdaptiveThrottlerConfig{
				SampleInterval:       100 * time.Millisecond,
				MaxMemoryPercent:     80,
				MaxCPUPercent:        70,
				RecoveryCPUThreshold: 75, // Higher than MaxCPUPercent
				MinRate:              10,
				MaxRate:              100,
				InitialRate:          50,
				BackoffFactor:        0.5,
				RecoveryFactor:       1.2,
			},
			wantErr: true,
		},
		{
			name: "Invalid RecoveryMemoryThreshold negative",
			config: AdaptiveThrottlerConfig{
				SampleInterval:          100 * time.Millisecond,
				MaxMemoryPercent:        80,
				RecoveryMemoryThreshold: -5,
				MinRate:                 10,
				MaxRate:                 100,
				InitialRate:             50,
				BackoffFactor:           0.5,
				RecoveryFactor:          1.2,
			},
			wantErr: true,
		},
		{
			name: "Invalid MaxCPUPercent negative",
			config: AdaptiveThrottlerConfig{
				SampleInterval:   100 * time.Millisecond,
				MaxMemoryPercent: 80,
				MaxCPUPercent:    -5,
				MinRate:          10,
				MaxRate:          100,
				InitialRate:      50,
				BackoffFactor:    0.5,
				RecoveryFactor:   1.2,
			},
			wantErr: true,
		},
		{
			name: "Invalid MinRate zero",
			config: AdaptiveThrottlerConfig{
				SampleInterval:   100 * time.Millisecond,
				MaxMemoryPercent: 80,
				MinRate:          0,
				MaxRate:          100,
				InitialRate:      50,
				BackoffFactor:    0.5,
				RecoveryFactor:   1.2,
			},
			wantErr: true,
		},
		{
			name: "Invalid MinRate negative",
			config: AdaptiveThrottlerConfig{
				SampleInterval:   100 * time.Millisecond,
				MaxMemoryPercent: 80,
				MinRate:          -5,
				MaxRate:          100,
				InitialRate:      50,
				BackoffFactor:    0.5,
				RecoveryFactor:   1.2,
			},
			wantErr: true,
		},
		{
			name: "Invalid MaxRate equals MinRate",
			config: AdaptiveThrottlerConfig{
				SampleInterval:   100 * time.Millisecond,
				MaxMemoryPercent: 80,
				MinRate:          10,
				MaxRate:          10,
				InitialRate:      10,
				BackoffFactor:    0.5,
				RecoveryFactor:   1.2,
			},
			wantErr: true,
		},
		{
			name: "Invalid MaxRate less than MinRate",
			config: AdaptiveThrottlerConfig{
				SampleInterval:   100 * time.Millisecond,
				MaxMemoryPercent: 80,
				MinRate:          100,
				MaxRate:          50,
				InitialRate:      75,
				BackoffFactor:    0.5,
				RecoveryFactor:   1.2,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.validate()
			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error but got: %v", err)
				}
			}
		})
	}
}

func TestAdaptiveThrottlerConfig_Validate_DefaultRecoveryThresholdsBelowTen(t *testing.T) {
	config := AdaptiveThrottlerConfig{
		SampleInterval:   100 * time.Millisecond,
		MaxMemoryPercent: 5, // < 10 to trigger 90% fallback
		MaxCPUPercent:    8, // < 10 to trigger 90% fallback
		MinRate:          1,
		MaxRate:          10,
		InitialRate:      5,
		BackoffFactor:    0.5,
		RecoveryFactor:   1.2,
	}

	if err := config.validate(); err != nil {
		t.Fatalf("expected config to validate, got: %v", err)
	}

	expectedMem := config.MaxMemoryPercent * 0.9
	expectedCPU := config.MaxCPUPercent * 0.9

	if config.RecoveryMemoryThreshold != expectedMem {
		t.Fatalf("expected RecoveryMemoryThreshold %.2f, got %.2f", expectedMem, config.RecoveryMemoryThreshold)
	}
	if config.RecoveryCPUThreshold != expectedCPU {
		t.Fatalf("expected RecoveryCPUThreshold %.2f, got %.2f", expectedCPU, config.RecoveryCPUThreshold)
	}
}

func TestDefaultAdaptiveThrottlerConfig(t *testing.T) {
	config := DefaultAdaptiveThrottlerConfig()
	if config == nil {
		t.Fatal("Expected config to be not nil")
	}
	if config.InitialRate != 1000 {
		t.Errorf("Expected InitialRate to be 1000, got %d", config.InitialRate)
	}
	if config.MaxMemoryPercent != 85.0 {
		t.Errorf("Expected MaxMemoryPercent to be 85.0, got %f", config.MaxMemoryPercent)
	}
	if config.MaxCPUPercent != 80.0 {
		t.Errorf("Expected MaxCPUPercent to be 80.0, got %f", config.MaxCPUPercent)
	}
	if config.RecoveryMemoryThreshold != 0 {
		t.Errorf("Expected RecoveryMemoryThreshold to be 0 (auto-calculated), got %f", config.RecoveryMemoryThreshold)
	}
	if config.RecoveryCPUThreshold != 0 {
		t.Errorf("Expected RecoveryCPUThreshold to be 0 (auto-calculated), got %f", config.RecoveryCPUThreshold)
	}
	if !config.EnableHysteresis {
		t.Errorf("Expected EnableHysteresis to be true, got %v", config.EnableHysteresis)
	}
}

func TestNewAdaptiveThrottler(t *testing.T) {
	at1, err := NewAdaptiveThrottler(nil)
	if err != nil {
		t.Fatalf("Expected no error with nil config, got: %v", err)
	}
	if at1 == nil {
		t.Fatal("Expected non-nil throttler")
	}
	if at1.config.InitialRate != 1000 {
		t.Errorf("Expected default InitialRate 1000, got %d", at1.config.InitialRate)
	}
	at1.close()

	config := DefaultAdaptiveThrottlerConfig()
	config.InitialRate = 500
	at2, err := NewAdaptiveThrottler(config)
	if err != nil {
		t.Fatalf("Expected no error with valid config, got: %v", err)
	}
	if at2.config.InitialRate != 500 {
		t.Errorf("Expected InitialRate 500, got %d", at2.config.InitialRate)
	}
	at2.close()

	invalidConfig := &AdaptiveThrottlerConfig{
		SampleInterval: 1 * time.Millisecond,
	}
	at3, err := NewAdaptiveThrottler(invalidConfig)
	if err == nil {
		at3.close()
		t.Fatal("Expected error with invalid config")
	}
}

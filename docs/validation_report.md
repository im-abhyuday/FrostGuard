# ColdChainIQ Dataset Validation Report

- **Generated**: `2026-10-30T12:35:23.966074Z`
- **Execution Wall Clock Time**: `186.49s`
- **Total Telemetry Readings**: `2,110,913`
- **Total Completed Shipments**: `2,998`
- **Fleet Size**: `50` active trucks
- **Temporal Span**: `2026-05-01T00:00:00Z` to `2026-05-30T23:59:00Z`

---

## 1. Class Imbalance & 30-Minute Prediction Window
- **Readings Out of Safe Range**: `1.575%`
- **Positive Class Ratio (Excursion within Next 30 Mins)**: `3.7%`
  *(Target band: 3% to 8%. Calibrated for XGBoost classification in Phase 4).*

---

## 2. Root Cause Attribution (Ground Truth)
| Root Cause Category | Events Count |
| :--- | :--- |
| `insufficient_precool` | 265 |
| `compressor_failure` | 141 |
| `door_left_open` | 131 |
| `long_stop_hot_ambient` | 78 |
| `compressor_degradation` | 4 |

---

## 3. Thermal Performance per Cargo Profile
| Cargo Type | Safe Band | Setpoint | Mean Temp | Std Dev | Median (p50) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **VACCINE** | `[2.0, 8.0] C` | `5.0 C` | `5.07 C` | `+/- 0.69 C` | `5.01 C` |
| **FROZEN_FOOD** | `[-25.0, -18.0] C` | `-21.0 C` | `-20.94 C` | `+/- 0.66 C` | `-21.0 C` |
| **DAIRY** | `[1.0, 4.0] C` | `2.5 C` | `2.54 C` | `+/- 0.61 C` | `2.5 C` |
| **FRESH_PRODUCE** | `[4.0, 10.0] C` | `7.0 C` | `7.03 C` | `+/- 0.61 C` | `6.99 C` |

---

## 4. Sanity Verification Checks
- [x] No NaNs in numeric or temporal features
- [x] Container temperatures bounded within physical thermodynamic limits (-40C to 60C)
- [x] Strict causal lookahead window preserves zero data leakage

# ColdChainIQ Container Physics Model Constants

The thermal dynamics model calculates temperature evolution using an explicit first-order Euler integration step over discrete interval $dt$ (default 60 seconds):

$$\frac{dT}{dt} = \frac{\dot{Q}_{\text{wall\_leak}} + \dot{Q}_{\text{door\_ingress}} - \dot{Q}_{\text{cooling}}}{C_{\text{thermal\_mass}}} + \eta_{\text{process\_noise}} + \delta_{\text{disturbance}}$$

---

### Core Physical Constants

| Constant | Value | Unit | Justification |
| :--- | :--- | :--- | :--- |
| `HEAT_LEAK_COEFF` | `18.0` | $\text{W/K}$ | Conductive/convective heat transmission coefficient across standard 40mm polyurethane-insulated refrigerated container walls ($U \approx 0.35\ \text{W}/(\text{m}^2\cdot\text{K})$ across $\approx 50\ \text{m}^2$ surface area). |
| `DOOR_HEAT_COEFF` | `180.0` | $\text{W/K}$ | Infiltration coefficient calibrated so that brief openings (1–5 min) remain hard negatives while extended openings (>10–15 min) in warm ambient trigger excursions at 3%–12% frequency. |
| `THERMOSTAT_BAND_C` | `0.5` | $^\circ\text{C}$ | Hysteresis deadband around setpoint to prevent compressor short-cycling and simulate mechanical relay switching behavior. |
| `PROCESS_NOISE_SIGMA` | `0.045` | $^\circ\text{C}$ | Gaussian stochastic standard deviation per minute modeling boundary-layer micro-turbulence, cargo airflow channeling, and local gradients. |
| `AIR_CORE_COUPLING` | `0.25` | $\text{hr}^{-1}$ | Biot-number dependent thermal coupling between circulating headspace air and high-density interior pallet cargo mass. |
| `SOLAR_HEAT_GAIN` | `320.0` | $\text{W}$ | Roof/body radiant absorption when ambient $\ge 28^\circ\text{C}$ scaling with temperature delta to represent highway daytime solar exposure. |

---

### Per-Truck Heterogeneity & Latent Factors (Seeded & Unobserved in Features)

| Factor | Range / Rate | Unit | Justification |
| :--- | :--- | :--- | :--- |
| `heat_leak_mult` | `[0.88, 1.12]` | scalar | Seeded per-truck variance in wall polyurethane foam aging, thermal bridging, and structural wear ($\pm 12\%$). |
| `cooling_power_mult` | `[0.88, 1.12]` | scalar | Seeded per-truck variance in compressor condenser efficiency, refrigerant charge, and fan age ($\pm 12\%$). |
| `latent_door_seal_factor` | `[0.90, 1.25]` | scalar | Unobserved gasket wear factor scaling wall and door infiltration without being present in sensor telemetry. |
| `latent_load_factor` | `[0.85, 1.15]` | scalar | Unobserved pallet mass variation (full load vs partial load) scaling effective heat capacity. |
| `thermal_pulse` | `0.12%` / step | $^\circ\text{C}$ | Rare transient thermal pulses ($\pm 0.15 - 0.35^\circ\text{C}$) modeling periodic evaporator defrost cycles and sudden air pockets. |
| `traffic_idle_heat_soak` | `comp_eff \le 0.05, \Delta T_{amb} = +3.5^\circ\text{C}` | scalar | Models lack of condenser ram-air airflow, engine idle rpm, and ground asphalt heat radiation during traffic jams. |
| `degradation_decay` | `\text{decay to } 0.08` | scalar | Power law degradation curve $(1.0 - 0.92 \cdot \text{frac}^{0.8})$ ensuring gradual precursor warming before failure. |

---

### Cargo Profile Constants

| Cargo Profile | Safe Band ($^\circ\text{C}$) | Setpoint ($^\circ\text{C}$) | Thermal Mass ($C_m$, $\text{kJ/K}$) | Cooling Power ($P_c$, $\text{kW}$) | Tolerance (min) | Justification |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **VACCINE** | `[2.0, 8.0]` | `5.0` | `1400.0` | `2.8` | `30` | High thermal sensitivity in compact packaging; strict WHO PQS prequalification guidelines. |
| **FROZEN_FOOD**| `[-25.0, -18.0]` | `-21.0` | `2800.0` | `4.5` | `60` | High latent/sensory thermal inertia of dense frozen solids; requires high-capacity sub-zero scroll compressors. |
| **DAIRY** | `[1.0, 4.0]` | `2.5` | `2100.0` | `3.2` | `45` | Liquid milk specific heat capacity ($\approx 3.93\ \text{kJ}/(\text{kg}\cdot\text{K})$) with narrow bacterial growth prevention window. |
| **FRESH_PRODUCE** | `[4.0, 10.0]` | `7.0` | `2500.0` | `3.0` | `120` | Moderate chilling buffer; respiration heat output offset by moisture-buffering vegetable crates. |

//! # Quotient Topological Coalesce (Colapso de Micro-Eventos no Espaço $X / \sim$)
//!
//! Condensa séries temporais de alta frequência e rajadas de micro-eventos financeiros
//! projetando-os em um espaço quociente topológico:
//!
//! $$x_i \sim x_j \iff \left( \frac{\sum_{r=i}^{k} |x_r - x_i|^p \, \mu(r)}{\sum_{r=i}^{k} \mu(r)} \right)^{\frac{1}{p}} \le \varepsilon \quad \forall k \in [i, j]$$
//!
//! Cada classe de equivalência contígua $[x_i]$ é colapsada no seu baricentro ponderado,
//! preservando todos os pontos de inflexão com amplitude $> \varepsilon$ e gerando um `RecordBatch`
//! Arrow compacto e denso.

use arrow_array::{Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Colapsa micro-eventos consecutivos em classes de equivalência baricêntricas $[x]$
/// sempre que a excursão na norma $L^p$ a partir da âncora permanece dentro da bola fechada $\overline{B}_\varepsilon$.
pub fn quotient_topological_coalesce(
    timestamps: &Int64Array,
    values: &Float64Array,
    volumes: &Float64Array,
    p_order: f64,
    epsilon_ball_radius: f64,
) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
    let n = values.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("window_start_ts", DataType::Int64, false),
        Field::new("window_end_ts", DataType::Int64, false),
        Field::new("quotient_barycenter_value", DataType::Float64, false),
        Field::new("coalesced_volume", DataType::Float64, false),
        Field::new("equivalence_class_size", DataType::Int64, false),
    ]));

    if n == 0 {
        return Ok(RecordBatch::new_empty(schema));
    }

    if timestamps.len() != n || volumes.len() != n {
        return Err(format!(
            "[QuotientCoalesce] Dimensões incompatíveis: timestamps={}, values={}, volumes={}",
            timestamps.len(),
            n,
            volumes.len()
        )
        .into());
    }

    let p = if p_order <= 0.0 { 1.0 } else { p_order };
    let ts_slice = timestamps.values();
    let val_slice = values.values();
    let vol_slice = volumes.values();

    let mut out_ts_start = Vec::new();
    let mut out_ts_end = Vec::new();
    let mut out_barycenter_val = Vec::new();
    let mut out_coalesced_vol = Vec::new();
    let mut out_event_count = Vec::new();

    let mut anchor_idx = 0usize;
    let mut anchor_val = val_slice[0];
    let mut lp_excursion_acc = 0.0_f64;
    let mut weighted_val_sum = val_slice[0] * vol_slice[0];
    let mut mass_sum = vol_slice[0];

    for i in 1..n {
        let v = val_slice[i];
        let w = vol_slice[i].max(1e-12);
        let diff_p = (v - anchor_val).abs().powf(p);

        let candidate_excursion = ((lp_excursion_acc + diff_p * w) / (mass_sum + w)).powf(1.0 / p);

        if candidate_excursion <= epsilon_ball_radius {
            // Permanece na mesma classe de equivalência topológica [x_anchor]
            lp_excursion_acc += diff_p * w;
            weighted_val_sum += v * w;
            mass_sum += w;
        } else {
            // Fronteira da bola B_epsilon atingida: emite o representante da classe no quociente X / ~
            out_ts_start.push(ts_slice[anchor_idx]);
            out_ts_end.push(ts_slice[i - 1]);
            out_barycenter_val.push(weighted_val_sum / mass_sum.max(1e-12));
            out_coalesced_vol.push(mass_sum);
            out_event_count.push((i - anchor_idx) as i64);

            // Inicia nova classe de equivalência com âncora em x_i
            anchor_idx = i;
            anchor_val = v;
            lp_excursion_acc = 0.0;
            weighted_val_sum = v * w;
            mass_sum = w;
        }
    }

    // Emite a última classe de equivalência
    out_ts_start.push(ts_slice[anchor_idx]);
    out_ts_end.push(ts_slice[n - 1]);
    out_barycenter_val.push(weighted_val_sum / mass_sum.max(1e-12));
    out_coalesced_vol.push(mass_sum);
    out_event_count.push((n - anchor_idx) as i64);

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(out_ts_start)),
            Arc::new(Int64Array::from(out_ts_end)),
            Arc::new(Float64Array::from(out_barycenter_val)),
            Arc::new(Float64Array::from(out_coalesced_vol)),
            Arc::new(Int64Array::from(out_event_count)),
        ],
    )?;

    Ok(batch)
}

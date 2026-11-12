//! Testes de Integração e Verificação Matemática do Operador KineCoalesce ($\mathcal{C}$)
//!
//! Cobre:
//! 1. `SheafCoalesce`: Homotopia de Calibre, Partição da Unidade e Extensão Harmônica de Dirichlet 1D.
//! 2. `SmoluchowskiCoalesce`: Coagulação com kernel exponencial e minimização de Dead Box Dilation.
//! 3. `QuotientCoalesce`: Colapso de micro-eventos no espaço quociente topológico $X / \sim$.

use arrow_array::{Array, Float64Array, Int64Array};
use kineplex_core::coalesce::{
    bitwise_sheaf_coalesce_pair, quotient_topological_coalesce, sheaf_harmonic_coalesce,
    smoluchowski_partition_coalesce, PartitionMetricDescriptor,
};

#[test]
fn test_sheaf_coalesce_gauge_alignment_and_unit_partition() {
    // Coluna 1: válida em [0..60], valor constante 100.0
    // Coluna 2: válida em [40..100], valor constante 115.0 (spread sistemático de +15.0)
    // Na interseção [40..60], ambos são válidos => Delta_{0,1} deve ser exatamente 15.0
    let n = 100usize;
    let mut v1 = Vec::with_capacity(n);
    let mut m1 = Vec::with_capacity(n);
    let mut v2 = Vec::with_capacity(n);
    let mut m2 = Vec::with_capacity(n);

    for i in 0..n {
        if i < 60 {
            v1.push(Some(100.0));
            m1.push(true);
        } else {
            v1.push(None);
            m1.push(false);
        }

        if i >= 40 {
            v2.push(Some(115.0));
            m2.push(true);
        } else {
            v2.push(None);
            m2.push(false);
        }
    }

    let col1 = Float64Array::from(v1);
    let col2 = Float64Array::from(v2);

    let precisions = vec![1.0, 1.0]; // Pesos iguais
    let result = sheaf_harmonic_coalesce(&[&col1, &col2], &precisions).expect("Sheaf Coalesce deve ter sucesso");

    assert_eq!(result.len(), n);
    assert_eq!(result.null_count(), 0, "O vetor resultante deve ser 100% denso");

    // Na região onde apenas col1 é válida [0..40), valor deve ser 100.0
    for i in 0..40 {
        assert!((result.value(i) - 100.0).abs() < 1e-9);
    }

    // Na região de interseção [40..60], a calibração de calibre remove o spread (+15.0)
    // col2 calibrada = 115.0 - 15.0 = 100.0. A média convexa deve ser 100.0!
    for i in 40..60 {
        assert!(
            (result.value(i) - 100.0).abs() < 1e-9,
            "Interseção calibrada deve ter valor 100.0 sem choques de degrau"
        );
    }

    // Na região onde apenas col2 é válida [60..100), o valor calibrado (115.0 - 15.0 = 100.0)
    // elimina a descontinuidade de salto L^inf!
    for i in 60..100 {
        assert!(
            (result.value(i) - 100.0).abs() < 1e-9,
            "Transição para col2 deve preservar o nível de calibre de col1"
        );
    }
}

#[test]
fn test_sheaf_coalesce_dirichlet_harmonic_vacuum_filling() {
    // Cria um buraco no meio (vazio total \Omega \setminus \bigcup U_k)
    // col1: válida em [0..20], valor 10.0
    // [20..40]: VAZIO TOTAL (ambas são nulas)
    // col2: válida em [40..60], valor 30.0
    let n = 60usize;
    let mut v1 = vec![None; n];
    let mut v2 = vec![None; n];

    for i in 0..20 {
        v1[i] = Some(10.0);
    }
    for i in 40..60 {
        v2[i] = Some(30.0);
    }

    let col1 = Float64Array::from(v1);
    let col2 = Float64Array::from(v2);

    let result = sheaf_harmonic_coalesce(&[&col1, &col2], &[1.0, 1.0]).unwrap();
    assert_eq!(result.null_count(), 0);

    // O intervalo [20..40] deve ser a solução harmônica linear exata de Dirichlet (\Delta_d f = 0)
    // ligando a fronteira esquerda (10.0 no índice 19) à direita (30.0 no índice 40)
    let left_val = result.value(19);
    let right_val = result.value(40);
    assert!((left_val - 10.0).abs() < 1e-9);
    assert!((right_val - 30.0).abs() < 1e-9);

    // No ponto médio i = 30, o valor deve ser aproximadamente 20.0
    let mid_val = result.value(29);
    assert!(
        (mid_val - 20.0).abs() < 1.5,
        "Interpolação harmônica de Dirichlet no ponto médio deve estar próxima de 20.0: obtido {}",
        mid_val
    );

    // A energia de Dirichlet não deve ter saltos abruptos (variação suave)
    for i in 20..39 {
        let diff = result.value(i + 1) - result.value(i);
        assert!(diff >= 0.0 && diff < 2.0, "Passo discreto harmônico deve ser monótono e suave");
    }
}

#[test]
fn test_bitwise_sheaf_pair_zero_branching() {
    let col_a = Float64Array::from(vec![Some(10.0), Some(20.0), None, None]);
    let col_b = Float64Array::from(vec![Some(15.0), None, Some(35.0), None]);

    let coalesced = bitwise_sheaf_coalesce_pair(&col_a, &col_b, 1.0, 1.0, 5.0);
    assert_eq!(coalesced.len(), 4);

    // i=0: ambos válidos, b calibrado = 15 - 5 = 10 => média = 10.0
    assert!((coalesced.value(0) - 10.0).abs() < 1e-9);
    // i=1: apenas A válido => 20.0
    assert!((coalesced.value(1) - 20.0).abs() < 1e-9);
    // i=2: apenas B válido => 35 - 5 = 30.0
    assert!((coalesced.value(2) - 30.0).abs() < 1e-9);
    // i=3: vazio => 0.0
    assert_eq!(coalesced.value(3), 0.0);
}

#[test]
fn test_smoluchowski_partition_coalesce_preserves_compact_iceberg_bounds() {
    // Cria 4 partições:
    // P0: [100.0, 200.0] massa 10MB
    // P1: [205.0, 300.0] massa 10MB (vizinho muito próximo de P0, espaço morto quase nulo)
    // P2: [9000.0, 9100.0] massa 10MB (distante)
    // P3: [9105.0, 9200.0] massa 10MB (vizinho próximo de P2)
    let p0 = PartitionMetricDescriptor {
        partition_id: 0,
        byte_size: 10_000_000,
        min_bound: 100.0,
        max_bound: 200.0,
        lp_signature: [10.0, 10.0, 10.0, 10.0],
        active: true,
    };
    let p1 = PartitionMetricDescriptor {
        partition_id: 1,
        byte_size: 10_000_000,
        min_bound: 205.0,
        max_bound: 300.0,
        lp_signature: [10.5, 10.2, 10.1, 10.0],
        active: true,
    };
    let p2 = PartitionMetricDescriptor {
        partition_id: 2,
        byte_size: 10_000_000,
        min_bound: 9000.0,
        max_bound: 9100.0,
        lp_signature: [50.0, 50.0, 50.0, 50.0],
        active: true,
    };
    let p3 = PartitionMetricDescriptor {
        partition_id: 3,
        byte_size: 10_000_000,
        min_bound: 9105.0,
        max_bound: 9200.0,
        lp_signature: [50.2, 50.1, 50.0, 50.0],
        active: true,
    };

    let descriptors = vec![p0, p1, p2, p3];
    let max_bytes = 25_000_000; // Permite fundir 2 partições em uma de 20MB
    let target_clusters = 2;

    let clusters = smoluchowski_partition_coalesce(descriptors, max_bytes, target_clusters, 0.1, 0.01);

    assert_eq!(clusters.len(), 2, "Deve coagular em exatamente 2 clusters");

    // Valida que P0 se fundiu com P1, e P2 com P3 (afinidade topológica e minimização de D_box)
    let has_cluster_0_1 = clusters.iter().any(|c| c.contains(&0) && c.contains(&1));
    let has_cluster_2_3 = clusters.iter().any(|c| c.contains(&2) && c.contains(&3));

    assert!(has_cluster_0_1, "P0 e P1 devem coagular juntos devido à proximidade de bounds");
    assert!(has_cluster_2_3, "P2 e P3 devem coagular juntos devido à proximidade de bounds");
}

#[test]
fn test_quotient_topological_coalesce_epsilon_collapse() {
    // 10 eventos consecutivos com variações pequenas (< 0.5)
    // Depois um salto para 100.0 (rompimento da bola epsilon)
    let timestamps = Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
    let values = Float64Array::from(vec![
        10.0, 10.1, 10.2, 10.1, 10.0, 9.9, 10.0, 10.1, 10.2, 10.0, // Bloco 1 (delta < 0.5)
        100.0, 100.1,                                                // Bloco 2 (salto)
    ]);
    let volumes = Float64Array::from(vec![1.0; 12]);

    let batch = quotient_topological_coalesce(&timestamps, &values, &volumes, 2.0, 0.5)
        .expect("Quotient coalesce deve ter sucesso");

    assert_eq!(batch.num_columns(), 5);
    assert_eq!(batch.num_rows(), 2, "Deve colapsar em exatamente 2 classes de equivalência topológica");

    let barycenters = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let class_sizes = batch
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    // Primeira classe: tamanho 10, baricentro em torno de 10.07
    assert_eq!(class_sizes.value(0), 10);
    assert!((barycenters.value(0) - 10.07).abs() < 0.1);

    // Segunda classe: tamanho 2, baricentro em torno de 100.05
    assert_eq!(class_sizes.value(1), 2);
    assert!((barycenters.value(1) - 100.05).abs() < 0.1);
}

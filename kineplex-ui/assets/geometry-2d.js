/**
 * KinePlex 2D Geometric Control & Metric Tensor Heatmap Specialist
 * =============================================================================
 * Implementa a visualização 2D completa do Controle Geométrico:
 *  1. Rede de Curvatura em Projeção Hiperbólica (Poincaré Disk Model $\mathbb{D}^2$)
 *     - 20 Nós renderizados com halos, rótulos e papéis funcionais NUMA.
 *     - 34 Arestas renderizadas como arcos geodésicos circulares ortogonais com
 *       gradiente térmico e espessura proporcionais à curvatura de Ollivier-Ricci
 *       ($R > 0$ esférico/verde-ciano vs $R < 0$ hiperbólico/vermelho-âmbar).
 *     - Traçador visual animado de geodésicas ótimas (Dijkstra riemanniano).
 *  2. Heatmap Interativo da Matriz do Tensor Métrico 20x20 ($g_{ij}$)
 *     - Grid 20x20 com gradiente de densidade métrica (azul marinho $0.0$ a ciano neon $1.0$).
 *     - Cabeçalhos de linhas e colunas N1 a N20 com badges de papel e NUMA.
 *     - Hover crosshairs e tooltip flutuante com dados exatos ($g_{uv}$, $\kappa$, $d_g$).
 *     - Clique para seleção de par de nós e destaque imediato da geodésica no mapa 2D.
 *  3. Gráfico 2D de Distribuição de Curvaturas (Curva Gaussiana com $\mu$, $\sigma$, Ponto Operacional)
 *  4. Conexão assíncrona com `/api/v1/geometry/tensor` com fallbacks completos e offline-first.
 * =============================================================================
 */

(function (window, document) {
  'use strict';

  // ===========================================================================
  // PALETA DE CORES E DESIGN SYSTEM KINEPLEX (CYBERNETIC / GLASSMORPHISM)
  // ===========================================================================
  const THEME = {
    bgDeep: '#071524',
    bgCard: '#0B1E33',
    bgSurface: '#0F2742',
    border: 'rgba(0, 167, 167, 0.25)',
    borderActive: 'rgba(0, 240, 255, 0.7)',
    kineCyan: '#00A7A7',
    kineNeon: '#00F0FF',
    kineBlue: '#123B5D',
    success: '#10B981',
    warning: '#F59E0B',
    danger: '#EF4444',
    purple: '#A855F7',
    slateMuted: '#64748B',
    textMain: '#E2E8F0',
    goldGlow: '#FFD700',
    roles: {
      ingress: '#A855F7',     // Roxo
      compute: '#00F0FF',     // Ciano Neon
      aggregation: '#F59E0B', // Âmbar
      parquet: '#10B981',     // Verde Esmeralda
      standby: '#38BDF8'      // Azul Céu
    }
  };

  // ===========================================================================
  // DADOS DA TOPOLOGIA REAL DE 20 NÓS DO CLUSTER DISTRIBUÍDO KINEPLEX
  // ===========================================================================
  const DEFAULT_NODES = [
    { id: 'kpx-node-01', name: 'Nó 1 (Semeador/Ingress A)', role: 'ingress', numa: 'NUMA 0', r: 0.32, theta: Math.PI },
    { id: 'kpx-node-02', name: 'Nó 2 (Receptor Zero-Copy A)', role: 'ingress', numa: 'NUMA 1', r: 0.30, theta: 2.2 },
    { id: 'kpx-node-03', name: 'Nó 3 (Compute Worker 1)', role: 'compute', numa: 'NUMA 2', r: 0.52, theta: 1.57 },
    { id: 'kpx-node-04', name: 'Nó 4 (Compute Worker 2)', role: 'compute', numa: 'NUMA 3', r: 0.52, theta: 1.05 },
    { id: 'kpx-node-05', name: 'Nó 5 (Aggregation Master A)', role: 'aggregation', numa: 'NUMA 0', r: 0.68, theta: 0.78 },
    { id: 'kpx-node-06', name: 'Nó 6 (Terminal Parquet A)', role: 'parquet', numa: 'NUMA 1', r: 0.83, theta: 0.52 },
    { id: 'kpx-node-07', name: 'Nó 7 (Relay & Shunt A)', role: 'standby', numa: 'NUMA 2', r: 0.72, theta: 2.40 },
    { id: 'kpx-node-08', name: 'Nó 8 (Compute Worker 3)', role: 'compute', numa: 'NUMA 3', r: 0.48, theta: 2.65 },
    { id: 'kpx-node-09', name: 'Nó 9 (Edge Gateway QUIC A)', role: 'ingress', numa: 'NUMA 0', r: 0.30, theta: 4.10 },
    { id: 'kpx-node-10', name: 'Nó 10 (Hot Standby Spare A)', role: 'standby', numa: 'NUMA 1', r: 0.76, theta: 3.45 },
    { id: 'kpx-node-11', name: 'Nó 11 (Semeador/Ingress B)', role: 'ingress', numa: 'NUMA 2', r: 0.34, theta: 5.20 },
    { id: 'kpx-node-12', name: 'Nó 12 (Compute SIMD AVX)', role: 'compute', numa: 'NUMA 3', r: 0.54, theta: 3.75 },
    { id: 'kpx-node-13', name: 'Nó 13 (Compute SIMD Worker 5)', role: 'compute', numa: 'NUMA 0', r: 0.52, theta: 4.71 },
    { id: 'kpx-node-14', name: 'Nó 14 (Compute SIMD Worker 6)', role: 'compute', numa: 'NUMA 1', r: 0.53, theta: 5.75 },
    { id: 'kpx-node-15', name: 'Nó 15 (Aggregation Shard 1)', role: 'aggregation', numa: 'NUMA 2', r: 0.68, theta: 6.00 },
    { id: 'kpx-node-16', name: 'Nó 16 (Aggregation Shard 2)', role: 'aggregation', numa: 'NUMA 3', r: 0.70, theta: 0.20 },
    { id: 'kpx-node-17', name: 'Nó 17 (Parquet Sink Part-1)', role: 'parquet', numa: 'NUMA 0', r: 0.83, theta: 0.00 },
    { id: 'kpx-node-18', name: 'Nó 18 (Parquet Sink Part-2)', role: 'parquet', numa: 'NUMA 1', r: 0.84, theta: 5.90 },
    { id: 'kpx-node-19', name: 'Nó 19 (Hot Standby Spare 2)', role: 'standby', numa: 'NUMA 2', r: 0.78, theta: 4.45 },
    { id: 'kpx-node-20', name: 'Nó 20 (Relay Mesh Gateway)', role: 'standby', numa: 'NUMA 3', r: 0.82, theta: 5.45 }
  ];

  // 34 conexões sinápticas com curvaturas Ollivier-Ricci e cargas
  const DEFAULT_EDGES = [
    { from: 0, to: 1, curvature: 0.042, load: 0.85, weight: 1.05 },
    { from: 0, to: 8, curvature: 0.028, load: 0.78, weight: 1.12 },
    { from: 0, to: 10, curvature: -0.015, load: 0.65, weight: 1.45 },
    { from: 1, to: 8, curvature: 0.035, load: 0.88, weight: 1.08 },
    { from: 8, to: 10, curvature: -0.022, load: 0.62, weight: 1.50 },
    { from: 1, to: 2, curvature: 0.031, load: 0.92, weight: 1.04 },
    { from: 8, to: 7, curvature: -0.018, load: 0.84, weight: 1.38 },
    { from: 10, to: 12, curvature: -0.025, load: 0.70, weight: 1.55 },
    { from: 2, to: 3, curvature: 0.048, load: 0.75, weight: 1.10 },
    { from: 2, to: 7, curvature: -0.012, load: 0.68, weight: 1.40 },
    { from: 2, to: 11, curvature: 0.022, load: 0.80, weight: 1.25 },
    { from: 3, to: 11, curvature: 0.029, load: 0.77, weight: 1.20 },
    { from: 7, to: 11, curvature: -0.030, load: 0.71, weight: 1.62 },
    { from: 7, to: 12, curvature: -0.038, load: 0.65, weight: 1.70 },
    { from: 11, to: 12, curvature: 0.034, load: 0.69, weight: 1.18 },
    { from: 11, to: 13, curvature: 0.037, load: 0.74, weight: 1.15 },
    { from: 12, to: 13, curvature: 0.041, load: 0.66, weight: 1.12 },
    { from: 3, to: 4, curvature: 0.025, load: 0.89, weight: 1.06 },
    { from: 11, to: 4, curvature: 0.021, load: 0.82, weight: 1.28 },
    { from: 11, to: 14, curvature: 0.032, load: 0.76, weight: 1.22 },
    { from: 4, to: 14, curvature: 0.019, load: 0.78, weight: 1.30 },
    { from: 13, to: 14, curvature: 0.045, load: 0.72, weight: 1.09 },
    { from: 13, to: 15, curvature: 0.031, load: 0.68, weight: 1.24 },
    { from: 14, to: 15, curvature: 0.028, load: 0.74, weight: 1.20 },
    { from: 4, to: 5, curvature: 0.036, load: 0.86, weight: 1.14 },
    { from: 14, to: 16, curvature: 0.024, load: 0.80, weight: 1.26 },
    { from: 15, to: 17, curvature: 0.038, load: 0.75, weight: 1.18 },
    { from: 5, to: 16, curvature: 0.018, load: 0.63, weight: 1.35 },
    { from: 16, to: 17, curvature: 0.027, load: 0.59, weight: 1.22 },
    { from: 10, to: 6, curvature: -0.042, load: 0.45, weight: 1.80 },
    { from: 6, to: 9, curvature: -0.035, load: 0.38, weight: 1.75 },
    { from: 9, to: 18, curvature: -0.028, load: 0.35, weight: 1.68 },
    { from: 18, to: 19, curvature: -0.020, load: 0.40, weight: 1.58 },
    { from: 19, to: 17, curvature: 0.026, load: 0.52, weight: 1.32 }
  ];

  // ===========================================================================
  // FUNÇÕES AUXILIARES MATEMÁTICAS & GEOMÉTRICAS
  // ===========================================================================

  /**
   * Gradiente térmico de densidade métrica g_ij:
   * 0.0 (azul marinho profundo #071524) -> 0.5 (ciano kine #00A7A7) -> 1.0 (ciano neon #00F0FF)
   */
  function getMetricThermalColor(val, alpha = 1.0) {
    const v = Math.max(0, Math.min(1, val));
    const stops = [
      { pos: 0.00, r: 7,   g: 21,  b: 36  }, // #071524
      { pos: 0.25, r: 18,  g: 59,  b: 93  }, // #123B5D
      { pos: 0.50, r: 0,   g: 167, b: 167 }, // #00A7A7
      { pos: 0.75, r: 0,   g: 215, b: 235 }, // Sky bright
      { pos: 1.00, r: 0,   g: 240, b: 255 }  // #00F0FF
    ];

    let c1 = stops[0], c2 = stops[stops.length - 1];
    for (let i = 0; i < stops.length - 1; i++) {
      if (v >= stops[i].pos && v <= stops[i + 1].pos) {
        c1 = stops[i];
        c2 = stops[i + 1];
        break;
      }
    }
    const t = (v - c1.pos) / (c2.pos - c1.pos || 1);
    const r = Math.round(c1.r + t * (c2.r - c1.r));
    const g = Math.round(c1.g + t * (c2.g - c1.g));
    const b = Math.round(c1.b + t * (c2.b - c1.b));
    return `rgba(${r}, ${g}, ${b}, ${alpha})`;
  }

  /**
   * Cor da aresta baseada na Curvatura de Ollivier-Ricci R:
   * R > 0 (esférico): verde/ciano neon
   * R < 0 (hiperbólico): âmbar/vermelho coral
   * R ~ 0 (plano): azul céu
   */
  function getCurvatureColor(r, alpha = 1.0) {
    if (r > 0.02) {
      return `rgba(0, 240, 255, ${alpha})`; // Ciano Neon
    } else if (r > 0.00) {
      return `rgba(16, 185, 129, ${alpha})`; // Verde Esmeralda
    } else if (r > -0.02) {
      return `rgba(56, 189, 248, ${alpha})`; // Azul Céu neutro
    } else if (r > -0.035) {
      return `rgba(245, 158, 11, ${alpha})`; // Âmbar
    } else {
      return `rgba(239, 68, 68, ${alpha})`;  // Vermelho
    }
  }

  /**
   * Calcula parâmetros analíticos do arco geodésico no Disco de Poincaré $\mathbb{D}^2$.
   * No modelo de Poincaré, geodésicas são círculos ortogonais ao círculo unitário.
   */
  function computePoincareGeodesicArc(u1, v1, u2, v2) {
    const cp = u1 * v2 - u2 * v1;
    // Se colineares com a origem (D = 0), a geodésica é uma reta radial
    if (Math.abs(cp) < 1e-5) {
      return { isLine: true };
    }

    const D = 2 * cp;
    const norm1 = u1 * u1 + v1 * v1 + 1;
    const norm2 = u2 * u2 + v2 * v2 + 1;

    // Centro do círculo ortogonal (u0, v0)
    const u0 = (norm1 * v2 - norm2 * v1) / D;
    const v0 = (u1 * norm2 - u2 * norm1) / D;

    const rc2 = u0 * u0 + v0 * v0 - 1;
    if (rc2 <= 0) {
      return { isLine: true };
    }

    const rc = Math.sqrt(rc2);
    const a1 = Math.atan2(v1 - v0, u1 - u0);
    const a2 = Math.atan2(v2 - v0, u2 - u0);

    // Determina qual arco conecta os pontos pelo interior do disco unitário
    let delta = a2 - a1;
    while (delta < 0) delta += 2 * Math.PI;
    while (delta >= 2 * Math.PI) delta -= 2 * Math.PI;

    const midA = a1 + delta / 2;
    const mx = u0 + rc * Math.cos(midA);
    const my = v0 + rc * Math.sin(midA);
    const isInside = (mx * mx + my * my) < 1.0;

    return {
      isLine: false,
      u0,
      v0,
      rc,
      a1,
      a2,
      anticlockwise: !isInside
    };
  }

  // ===========================================================================
  // CLASSE PRINCIPAL: KinePlex2DGeometry
  // ===========================================================================
  class KinePlex2DGeometry {
    constructor(options = {}) {
      this.containerId = options.containerId || 'kineplex-2d-geometry-container';
      this.nodes = JSON.parse(JSON.stringify(DEFAULT_NODES));
      this.edges = JSON.parse(JSON.stringify(DEFAULT_EDGES));
      this.matrix = [];
      this.volumeInvariant = 341.28;
      this.avgCurvature = 0.023;
      this.routeEpoch = 14;

      // Estado Interativo
      this.selectedPair = [0, 5]; // Par padrão: Nó 1 -> Nó 6
      this.hoveredCell = null;
      this.hoveredNode = null;
      this.activeGeodesicPath = [];
      this.particles = [];
      this.panOffset = { x: 0, y: 0 };
      this.zoom = 1.0;
      this.isDragging = false;
      this.dragStart = { x: 0, y: 0 };
      this.filterMode = 'all'; // 'all', 'spherical', 'hyperbolic'
      this.showLabels = true;

      // Elementos do DOM
      this.rootContainer = null;
      this.poincareCanvas = null;
      this.poincareCtx = null;
      this.heatmapCanvas = null;
      this.heatmapCtx = null;
      this.tooltipEl = null;

      // Controle de Animação e Polling
      this.animFrameId = null;
      this.pollInterval = null;
      this.isDestroyed = false;

      // Inicialização
      this.initMatrix();
      this.initParticles();
      this.updateGeodesicPath(this.selectedPair[0], this.selectedPair[1]);
    }

    /**
     * Inicializa a matriz 20x20 do Tensor Métrico g_ij
     */
    initMatrix() {
      const n = 20;
      this.matrix = Array.from({ length: n }, () => Array(n).fill(0.0));

      // Diagonal g_ii = 1.0 (normalização invariante do espaço tangente local)
      for (let i = 0; i < n; i++) {
        this.matrix[i][i] = 1.0;
      }

      // Conexões diretas
      for (const e of this.edges) {
        if (e.from < n && e.to < n) {
          // Coeficiente métrico dependente de curvatura de Ollivier-Ricci e carga
          const gVal = Math.min(0.98, Math.max(0.20, 0.62 + e.curvature * 4.5 + e.load * 0.22));
          this.matrix[e.from][e.to] = parseFloat(gVal.toFixed(4));
          this.matrix[e.to][e.from] = parseFloat(gVal.toFixed(4));
        }
      }

      // Conexões multi-hop: decay riemanniano e^{-lambda * d_geodesic}
      for (let i = 0; i < n; i++) {
        for (let j = 0; j < n; j++) {
          if (i !== j && this.matrix[i][j] === 0.0) {
            const pathInfo = this.dijkstra(i, j);
            const dist = pathInfo.totalWeight;
            const decay = Math.exp(-0.45 * dist);
            const gVal = Math.min(0.50, Math.max(0.05, decay));
            this.matrix[i][j] = parseFloat(gVal.toFixed(4));
          }
        }
      }
    }

    /**
     * Inicializa partículas energéticas viajando pelas arestas hiperbólicas
     */
    initParticles() {
      this.particles = [];
      const particleCount = 42;
      for (let i = 0; i < particleCount; i++) {
        const edgeIdx = Math.floor(Math.random() * this.edges.length);
        this.particles.push({
          edgeIdx,
          progress: Math.random(),
          speed: 0.004 + Math.random() * 0.008,
          size: 1.5 + Math.random() * 1.8
        });
      }
    }

    /**
     * Monta o componente completo no DOM
     */
    mount(targetElement) {
      if (typeof targetElement === 'string') {
        targetElement = document.getElementById(targetElement);
      }

      if (!targetElement && this.containerId) {
        targetElement = document.getElementById(this.containerId);
      }

      if (!targetElement) {
        targetElement = document.getElementById('kineplex-2d-geometry-container');
      }

      if (!targetElement) {
        // Tenta encontrar aba geométrica ou insere no final de tab-geometric
        const tabGeo = document.getElementById('tab-geometric');
        if (tabGeo) {
          const wrapper = document.createElement('div');
          wrapper.id = this.containerId || 'kineplex-2d-geometry-container';
          wrapper.className = 'w-full my-6 min-h-[520px]';
          tabGeo.appendChild(wrapper);
          targetElement = wrapper;
        } else {
          console.warn('[KinePlex2DGeometry] Contêiner alvo não encontrado. Criando contêiner dinâmico.');
          const dynWrapper = document.createElement('div');
          dynWrapper.id = this.containerId || 'kineplex-2d-geometry-container';
          if (document.body) {
            document.body.appendChild(dynWrapper);
          }
          targetElement = dynWrapper;
        }
      }

      this.rootContainer = targetElement;
      this.renderDOMStructure();
      this.bindCanvases();
      this.bindEvents();
      this.setupResizeObserver();
      this.startAnimationLoop();
      this.fetchTensorData();

      // Polling a cada 4 segundos com fallbacks
      this.pollInterval = setInterval(() => {
        if (!this.isDestroyed) this.fetchTensorData();
      }, 4000);

      // Sincroniza com gráficos existentes no index.html se presentes
      this.syncExistingCurvatureChart();
    }

    /**
     * Renderiza o HTML responsivo da seção 2D
     */
    renderDOMStructure() {
      this.rootContainer.innerHTML = `
        <div class="glass-panel rounded-2xl p-6 border border-kine-border relative space-y-6 shadow-2xl">
          
          <!-- Header do Controle Geométrico 2D -->
          <div class="flex items-center justify-between flex-wrap gap-4 border-b border-kine-border/60 pb-4">
            <div>
              <div class="flex items-center space-x-3">
                <span class="w-3 h-3 rounded-full bg-kine-neon shadow-lg shadow-kine-neon animate-pulse"></span>
                <h3 class="text-base font-bold text-white font-mono uppercase tracking-wider">
                  2D CURVATURE NETWORK &amp; METRIC TENSOR HEATMAP
                </h3>
                <span class="text-xs px-2.5 py-0.5 rounded-full bg-kine-cyan/20 border border-kine-cyan/50 text-kine-neon font-mono">
                  POINCARÉ DISK 𝔻²
                </span>
              </div>
              <p class="text-xs text-slate-400 mt-1 font-mono">
                Visualização bidimensional com métrica hiperbólica ds², curvatura de Ollivier-Ricci κ e tensor métrico riemanniano gᵢⱼ (20×20).
              </p>
            </div>

            <!-- Toolbar de Filtros e Controles -->
            <div class="flex items-center space-x-2 text-xs font-mono flex-wrap gap-2">
              <div class="px-3 py-1 rounded-lg bg-kine-surface border border-kine-border text-slate-300">
                INVARIANTE: <strong class="text-kine-neon" id="geo2d-vol-inv">${this.volumeInvariant.toFixed(2)}</strong>
              </div>
              <div class="px-3 py-1 rounded-lg bg-kine-surface border border-kine-border text-slate-300">
                ÉPOCA: <strong class="text-emerald-400" id="geo2d-epoch">Epoch ${this.routeEpoch}</strong>
              </div>
              <button id="btn-geo2d-reset-cam" class="px-2.5 py-1 rounded bg-kine-card hover:bg-kine-surface border border-kine-border text-slate-300 transition">
                Reset Visão
              </button>
              <button id="btn-geo2d-labels" class="px-2.5 py-1 rounded bg-kine-cyan/20 border border-kine-cyan/50 text-kine-neon transition">
                Rótulos: ON
              </button>
              <select id="select-geo2d-filter" class="px-2.5 py-1 rounded bg-kine-surface border border-kine-border text-slate-300 focus:outline-none cursor-pointer">
                <option value="all">Todas as Curvaturas (34)</option>
                <option value="spherical">Esféricas (R &gt; 0)</option>
                <option value="hyperbolic">Hiperbólicas (R &lt; 0)</option>
              </select>
            </div>
          </div>

          <!-- Grid Principal: Poincaré Disk 2D (7 Cols) vs Metric Tensor Heatmap (5 Cols) -->
          <div class="grid grid-cols-1 lg:grid-cols-12 gap-6">

            <!-- Coluna 1: Poincaré Disk 2D Canvas (7 Cols) -->
            <div class="lg:col-span-7 flex flex-col space-y-3">
              <div class="flex items-center justify-between text-xs font-mono text-slate-300">
                <span class="flex items-center space-x-2">
                  <span class="w-2 h-2 rounded-full bg-purple-400"></span>
                  <strong>PROJEÇÃO NO DISCO DE POINCARÉ</strong>
                </span>
                <span class="text-[11px] text-slate-400">Clique para selecionar par • Scroll para zoom</span>
              </div>

              <!-- Canvas Container do Poincaré Disk -->
              <div class="relative w-full h-[460px] rounded-xl bg-[#040C14] border border-kine-border/80 overflow-hidden flex items-center justify-center shadow-inner" role="region" aria-label="Área de Projeção do Disco de Poincaré">
                <canvas id="poincareCanvas" class="w-full h-full cursor-grab active:cursor-grabbing focus:outline-none focus-visible:ring-2 focus-visible:ring-kine-cyan" role="img" aria-label="Visualização em 2D do Disco Hiperbólico de Poincaré projetando nós e geodésicas da malha neural" tabindex="0">
                  <p class="sr-only">Disco de Poincaré mapeando a topologia hiperbólica com curvatura negativa e geodésicas Riemannianas calculadas via transporte ótimo de Ollivier-Ricci.</p>
                </canvas>

                <!-- Watermark Matemático -->
                <div class="absolute bottom-3 left-3 text-[10px] font-mono text-slate-500/70 pointer-events-none">
                  ds² = 4 (dx² + dy²) / (1 - (x² + y²))² | Ollivier-Ricci Curvature κ(e)
                </div>

                <!-- Legenda de Curvatura Overlay -->
                <div class="absolute top-3 left-3 bg-kine-deep/85 backdrop-blur border border-kine-border/60 p-2.5 rounded-lg text-[10px] font-mono space-y-1 z-10 pointer-events-none">
                  <div class="flex items-center space-x-2">
                    <span class="w-2.5 h-2.5 rounded bg-kine-neon"></span>
                    <span class="text-slate-300">R &gt; +0.02 (Esférico / Contrativo)</span>
                  </div>
                  <div class="flex items-center space-x-2">
                    <span class="w-2.5 h-2.5 rounded bg-emerald-400"></span>
                    <span class="text-slate-300">R &gt; 0.00 (Positivo Suave)</span>
                  </div>
                  <div class="flex items-center space-x-2">
                    <span class="w-2.5 h-2.5 rounded bg-amber-400"></span>
                    <span class="text-slate-300">R &lt; 0.00 (Hiperbólico / Dispersivo)</span>
                  </div>
                  <div class="flex items-center space-x-2">
                    <span class="w-2.5 h-2.5 rounded bg-rose-500"></span>
                    <span class="text-slate-300">R &lt; -0.03 (Gargalo Topológico)</span>
                  </div>
                </div>

                <!-- Rota Geodésica Ativa Badge -->
                <div id="geo2d-active-route-badge" class="absolute top-3 right-3 bg-kine-deep/90 backdrop-blur border border-kine-cyan/60 px-3 py-1.5 rounded-lg text-[11px] font-mono text-slate-200 z-10">
                  Geodésica: <strong class="text-kine-neon font-bold">N1 → N6</strong> (Ativa)
                </div>
              </div>

              <!-- Mini Status Geodésico -->
              <div class="p-3 rounded-xl bg-kine-surface/50 border border-kine-border flex items-center justify-between text-xs font-mono text-slate-300">
                <div class="flex items-center space-x-2">
                  <span class="text-kine-neon font-bold">ROTA SELECIONADA:</span>
                  <span id="geo2d-path-display" class="text-white">Nó 1 → Nó 2 → Nó 3 → Nó 6</span>
                </div>
                <div>
                  Comprimento Geodésico: <strong class="text-emerald-400" id="geo2d-path-weight">0.042 ms</strong>
                </div>
              </div>
            </div>

            <!-- Coluna 2: Heatmap 20x20 do Tensor Métrico g_ij (5 Cols) -->
            <div class="lg:col-span-5 flex flex-col space-y-3">
              <div class="flex items-center justify-between text-xs font-mono text-slate-300">
                <span class="flex items-center space-x-2">
                  <span class="w-2 h-2 rounded-full bg-kine-cyan"></span>
                  <strong>MATRIZ DO TENSOR MÉTRICO 20×20 (gᵢⱼ)</strong>
                </span>
                <span class="text-[11px] text-slate-400">Passe o mouse ou clique</span>
              </div>

              <!-- Canvas Container do Heatmap 20x20 -->
              <div class="relative w-full h-[460px] rounded-xl bg-[#040C14] border border-kine-border/80 overflow-hidden flex items-center justify-center shadow-inner" role="region" aria-label="Área da Matriz do Tensor Métrico 20 por 20">
                <canvas id="heatmapCanvas" class="w-full h-full cursor-pointer focus:outline-none focus-visible:ring-2 focus-visible:ring-kine-cyan" role="img" aria-label="Matriz do Tensor Métrico 20 por 20 em mapa de calor bidimensional" tabindex="0">
                  <p class="sr-only">Mapa de calor 20 por 20 representando os coeficientes da métrica Riemanniana g_ij entre os 20 nós do cluster.</p>
                </canvas>

                <!-- Tooltip Flutuante -->
                <div id="heatmapTooltip" class="absolute pointer-events-none p-3 rounded-xl glass-card border border-kine-cyan/70 text-xs font-mono shadow-2xl z-30 hidden" style="min-width: 210px;">
                  <div class="flex items-center justify-between border-b border-kine-border/60 pb-1.5 mb-1.5">
                    <span id="tt-title" class="font-bold text-kine-neon">g(u, v)</span>
                    <span id="tt-val" class="px-1.5 py-0.5 rounded bg-kine-cyan/20 text-kine-neon text-[10px]">0.8500</span>
                  </div>
                  <div class="space-y-1 text-slate-300 text-[11px]">
                    <div>Nós: <strong id="tt-nodes" class="text-white">N1 ↔ N2</strong></div>
                    <div>Curvatura: <strong id="tt-curv" class="text-emerald-400">+0.035</strong></div>
                    <div>Distância: <strong id="tt-dist" class="text-kine-neon">0.24ms</strong></div>
                    <div>Tipo: <span id="tt-type" class="text-slate-400">Sinapse Direta</span></div>
                  </div>
                  <div class="mt-2 pt-1 border-t border-kine-border/40 text-[9px] text-slate-400">
                    Clique para traçar geodésica no mapa
                  </div>
                </div>
              </div>

              <!-- Barra Térmica de Referência -->
              <div class="p-2.5 rounded-xl bg-kine-surface/50 border border-kine-border flex items-center justify-between text-[11px] font-mono text-slate-400">
                <span>0.0 (Vazio)</span>
                <div class="w-48 h-2.5 rounded-full overflow-hidden mx-2 shadow-inner" style="background: linear-gradient(to right, #071524 0%, #123B5D 25%, #00A7A7 50%, #00D7EB 75%, #00F0FF 100%);"></div>
                <span class="text-kine-neon font-bold">1.0 (Tensor Máx)</span>
              </div>
            </div>

          </div>

          <!-- Painel Inferior: Curvatura Gaussiana e Estatísticas de Operação -->
          <div class="p-4 rounded-xl bg-kine-surface/40 border border-kine-border grid grid-cols-1 md:grid-cols-3 gap-4 font-mono text-xs text-slate-300">
            <div class="flex items-center space-x-3">
              <div class="w-8 h-8 rounded-lg bg-kine-cyan/20 border border-kine-cyan/40 flex items-center justify-center text-kine-neon font-bold">
                R̄
              </div>
              <div>
                <div class="text-[10px] text-slate-400">CURVATURA MÉDIA DE RICCI</div>
                <div class="text-base font-bold text-kine-neon">${this.avgCurvature.toFixed(3)}</div>
              </div>
            </div>

            <div class="flex items-center space-x-3">
              <div class="w-8 h-8 rounded-lg bg-emerald-950/60 border border-emerald-500/40 flex items-center justify-center text-emerald-400 font-bold">
                σ
              </div>
              <div>
                <div class="text-[10px] text-slate-400">DESVIO PADRÃO TOPOLÓGICO</div>
                <div class="text-base font-bold text-emerald-300">0.018</div>
              </div>
            </div>

            <div class="flex items-center space-x-3">
              <div class="w-8 h-8 rounded-lg bg-purple-950/60 border border-purple-500/40 flex items-center justify-center text-purple-300 font-bold">
                dim
              </div>
              <div>
                <div class="text-[10px] text-slate-400">DIMENSÃO RIEMANNIANA</div>
                <div class="text-base font-bold text-white">20x20 Coeficientes (400 Tensores)</div>
              </div>
            </div>
          </div>

        </div>
      `;
    }

    /**
     * Conecta elementos canvas e configura suporte Retina (devicePixelRatio)
     */
    bindCanvases() {
      this.poincareCanvas = document.getElementById('poincareCanvas');
      this.heatmapCanvas = document.getElementById('heatmapCanvas');
      this.tooltipEl = document.getElementById('heatmapTooltip');

      if (this.poincareCanvas) {
        this.poincareCtx = this.poincareCanvas.getContext('2d');
        this.resizeCanvas(this.poincareCanvas);
      }

      if (this.heatmapCanvas) {
        this.heatmapCtx = this.heatmapCanvas.getContext('2d');
        this.resizeCanvas(this.heatmapCanvas);
      }

      window.addEventListener('resize', () => {
        this.resize();
      });
    }

    resize() {
      if (this.poincareCanvas) this.resizeCanvas(this.poincareCanvas);
      if (this.heatmapCanvas) this.resizeCanvas(this.heatmapCanvas);
      this.drawHeatmap();
      this.drawPoincareDisk();
    }

    resizeCanvas(canvas) {
      if (!canvas) return;
      const rect = canvas.getBoundingClientRect();
      const dpr = window.devicePixelRatio || 1;

      let width = rect.width;
      let height = rect.height;

      // Se rect.width === 0 ou rect.height === 0, busca dimensões do elemento pai ou fallbacks
      if (width <= 0 && canvas.parentElement) {
        width = canvas.parentElement.clientWidth || canvas.parentElement.getBoundingClientRect().width;
      }
      if (height <= 0 && canvas.parentElement) {
        height = canvas.parentElement.clientHeight || canvas.parentElement.getBoundingClientRect().height;
      }

      // Fallbacks mínimos caso as dimensões ainda sejam 0
      if (width <= 0) width = 640;
      if (height <= 0) height = 460;

      canvas.width = Math.round(width * dpr);
      canvas.height = Math.round(height * dpr);
    }

    setupResizeObserver() {
      if (typeof window.ResizeObserver !== 'undefined') {
        this.resizeObserver = new window.ResizeObserver((entries) => {
          let needsResize = false;
          for (const entry of entries) {
            const cr = entry.contentRect;
            if (cr && (cr.width > 0 || cr.height > 0)) {
              needsResize = true;
              break;
            }
          }
          if (needsResize) {
            this.resize();
          }
        });

        if (this.rootContainer) {
          this.resizeObserver.observe(this.rootContainer);
        }
        if (this.poincareCanvas && this.poincareCanvas.parentElement) {
          this.resizeObserver.observe(this.poincareCanvas.parentElement);
        }
        if (this.heatmapCanvas && this.heatmapCanvas.parentElement) {
          this.resizeObserver.observe(this.heatmapCanvas.parentElement);
        }
        const tabGeo = document.getElementById('tab-geometric');
        if (tabGeo && tabGeo !== this.rootContainer) {
          this.resizeObserver.observe(tabGeo);
        }
      }

      // Listener no botão da aba para garantir redimensionamento imediato ao abrir
      const navTabGeo = document.getElementById('nav-tab-geometric');
      if (navTabGeo) {
        navTabGeo.addEventListener('click', () => {
          setTimeout(() => this.resize(), 50);
          setTimeout(() => this.resize(), 150);
          setTimeout(() => this.resize(), 300);
        });
      }
    }

    /**
     * Vincula listeners de interação aos componentes
     */
    bindEvents() {
      // 1. Controles do Header
      const btnReset = document.getElementById('btn-geo2d-reset-cam');
      if (btnReset) {
        btnReset.addEventListener('click', () => {
          this.panOffset = { x: 0, y: 0 };
          this.zoom = 1.0;
        });
      }

      const btnLabels = document.getElementById('btn-geo2d-labels');
      if (btnLabels) {
        btnLabels.addEventListener('click', () => {
          this.showLabels = !this.showLabels;
          btnLabels.innerText = `Rótulos: ${this.showLabels ? 'ON' : 'OFF'}`;
        });
      }

      const selectFilter = document.getElementById('select-geo2d-filter');
      if (selectFilter) {
        selectFilter.addEventListener('change', (e) => {
          this.filterMode = e.target.value;
        });
      }

      // 2. Interações com Poincaré Canvas (Pan, Zoom, Clique)
      if (this.poincareCanvas) {
        this.poincareCanvas.addEventListener('mousedown', (e) => {
          this.isDragging = true;
          this.dragStart = { x: e.clientX - this.panOffset.x, y: e.clientY - this.panOffset.y };
        });

        window.addEventListener('mouseup', () => {
          this.isDragging = false;
        });

        this.poincareCanvas.addEventListener('mousemove', (e) => {
          if (this.isDragging) {
            this.panOffset.x = e.clientX - this.dragStart.x;
            this.panOffset.y = e.clientY - this.dragStart.y;
          } else {
            // Detecção de hover em nós
            const rect = this.poincareCanvas.getBoundingClientRect();
            const mouseX = e.clientX - rect.left;
            const mouseY = e.clientY - rect.top;
            const hoveredNode = this.getNodeAtMouse(mouseX, mouseY);
            this.hoveredNode = hoveredNode;
            this.poincareCanvas.style.cursor = hoveredNode ? 'pointer' : (this.isDragging ? 'grabbing' : 'grab');
          }
        });

        this.poincareCanvas.addEventListener('wheel', (e) => {
          e.preventDefault();
          const zoomFactor = e.deltaY < 0 ? 1.08 : 0.92;
          this.zoom = Math.max(0.6, Math.min(2.5, this.zoom * zoomFactor));
        }, { passive: false });

        this.poincareCanvas.addEventListener('click', (e) => {
          const rect = this.poincareCanvas.getBoundingClientRect();
          const mouseX = e.clientX - rect.left;
          const mouseY = e.clientY - rect.top;
          const nodeIdx = this.getNodeAtMouse(mouseX, mouseY);

          if (nodeIdx !== null) {
            // Seleciona par geodésico
            const [u, v] = this.selectedPair;
            if (u !== nodeIdx) {
              this.highlightGeodesic(u, nodeIdx);
            }
          }
        });
      }

      // 3. Interações com Heatmap Canvas (Hover crosshair, Tooltip, Clique)
      if (this.heatmapCanvas) {
        this.heatmapCanvas.addEventListener('mousemove', (e) => {
          const rect = this.heatmapCanvas.getBoundingClientRect();
          const mouseX = e.clientX - rect.left;
          const mouseY = e.clientY - rect.top;

          const cell = this.getCellAtMouse(mouseX, mouseY);
          this.hoveredCell = cell;

          if (cell && this.tooltipEl) {
            const [u, v] = cell;
            const val = this.matrix[u][v] || 0.0;
            const edge = this.findEdge(u, v);
            const pathInfo = this.dijkstra(u, v);

            document.getElementById('tt-title').innerText = `g(${u + 1}, ${v + 1})`;
            document.getElementById('tt-val').innerText = val.toFixed(4);
            document.getElementById('tt-nodes').innerText = `N${u + 1} ↔ N${v + 1}`;
            
            const curvText = edge ? (edge.curvature > 0 ? `+${edge.curvature.toFixed(3)} (Esférica)` : `${edge.curvature.toFixed(3)} (Hiperbólica)`) : 'Geodésica Multi-hop';
            document.getElementById('tt-curv').innerText = curvText;
            document.getElementById('tt-dist').innerText = `${pathInfo.totalWeight.toFixed(3)} ms`;
            document.getElementById('tt-type').innerText = edge ? 'Sinapse Direta (QUIC)' : `${pathInfo.path.length - 1} Saltos Geodésicos`;

            // Posiciona tooltip dentro do container
            const tipX = Math.min(rect.width - 230, Math.max(10, mouseX + 15));
            const tipY = Math.min(rect.height - 130, Math.max(10, mouseY - 20));
            this.tooltipEl.style.left = `${tipX}px`;
            this.tooltipEl.style.top = `${tipY}px`;
            this.tooltipEl.classList.remove('hidden');
          } else if (this.tooltipEl) {
            this.tooltipEl.classList.add('hidden');
          }

          this.drawHeatmap();
        });

        this.heatmapCanvas.addEventListener('mouseleave', () => {
          this.hoveredCell = null;
          if (this.tooltipEl) this.tooltipEl.classList.add('hidden');
          this.drawHeatmap();
        });

        this.heatmapCanvas.addEventListener('click', (e) => {
          const rect = this.heatmapCanvas.getBoundingClientRect();
          const mouseX = e.clientX - rect.left;
          const mouseY = e.clientY - rect.top;
          const cell = this.getCellAtMouse(mouseX, mouseY);
          if (cell) {
            const [u, v] = cell;
            this.highlightGeodesic(u, v);
          }
        });
      }
    }

    /**
     * Algoritmo de Dijkstra para cálculo da geodésica mínima no grafo métrico
     */
    dijkstra(startIdx, endIdx) {
      const n = 20;
      const dist = Array(n).fill(Infinity);
      const prev = Array(n).fill(null);
      const visited = Array(n).fill(false);

      dist[startIdx] = 0;

      for (let i = 0; i < n; i++) {
        let u = -1;
        for (let j = 0; j < n; j++) {
          if (!visited[j] && (u === -1 || dist[j] < dist[u])) {
            u = j;
          }
        }

        if (dist[u] === Infinity || u === endIdx) break;
        visited[u] = true;

        for (const edge of this.edges) {
          let neighbor = null;
          if (edge.from === u) neighbor = edge.to;
          else if (edge.to === u) neighbor = edge.from;

          if (neighbor !== null && !visited[neighbor]) {
            const weight = edge.weight || 1.0;
            const newDist = dist[u] + weight;
            if (newDist < dist[neighbor]) {
              dist[neighbor] = newDist;
              prev[neighbor] = u;
            }
          }
        }
      }

      // Reconstrução do caminho
      const path = [];
      let curr = endIdx;
      while (curr !== null) {
        path.unshift(curr);
        curr = prev[curr];
      }

      if (path.length === 1 && path[0] !== startIdx) {
        return { path: [startIdx, endIdx], totalWeight: 2.0 };
      }

      return {
        path: path.length ? path : [startIdx, endIdx],
        totalWeight: dist[endIdx] !== Infinity ? dist[endIdx] : 1.5
      };
    }

    /**
     * Atualiza e destaca a geodésica ótima entre nós u e v
     */
    highlightGeodesic(u, v) {
      if (u < 0 || u >= 20 || v < 0 || v >= 20) return;
      this.selectedPair = [u, v];
      this.updateGeodesicPath(u, v);
      this.drawHeatmap();

      // Atualiza badge de rota
      const badge = document.getElementById('geo2d-active-route-badge');
      if (badge) {
        badge.innerHTML = `Geodésica: <strong class="text-kine-neon font-bold">N${u + 1} → N${v + 1}</strong> (Ativa)`;
      }

      // Notifica inspetor do index.html se disponível
      if (window.inspectRoutePath) {
        window.inspectRoutePath(`#GC-${(u + v * 3) % 50 + 10}`);
      }
    }

    updateGeodesicPath(u, v) {
      const res = this.dijkstra(u, v);
      this.activeGeodesicPath = res.path;

      const pathDisplay = document.getElementById('geo2d-path-display');
      if (pathDisplay) {
        pathDisplay.innerText = res.path.map(n => `Nó ${n + 1}`).join(' → ');
      }

      const weightDisplay = document.getElementById('geo2d-path-weight');
      if (weightDisplay) {
        weightDisplay.innerText = `${res.totalWeight.toFixed(3)} ms`;
      }
    }

    findEdge(u, v) {
      return this.edges.find(e => (e.from === u && e.to === v) || (e.from === v && e.to === u)) || null;
    }

    /**
     * Localiza nó sob o cursor no canvas de Poincaré
     */
    getNodeAtMouse(mouseX, mouseY) {
      if (!this.poincareCanvas) return null;
      const rect = this.poincareCanvas.getBoundingClientRect();
      const dpr = window.devicePixelRatio || 1;
      let width = rect.width;
      let height = rect.height;
      if (width < 50 || height < 50) {
        width = this.poincareCanvas.width / dpr || 640;
        height = this.poincareCanvas.height / dpr || 460;
      }
      const cx = width / 2 + this.panOffset.x;
      const cy = height / 2 + this.panOffset.y;
      const rDisk = Math.min(width, height) * 0.44 * this.zoom;

      for (let i = 0; i < this.nodes.length; i++) {
        const node = this.nodes[i];
        const nx = cx + node.r * rDisk * Math.cos(node.theta);
        const ny = cy + node.r * rDisk * Math.sin(node.theta);
        const dx = mouseX - nx;
        const dy = mouseY - ny;
        if (dx * dx + dy * dy < 20 * 20) {
          return i;
        }
      }
      return null;
    }

    /**
     * Localiza célula (u, v) sob o cursor no canvas de heatmap
     */
    getCellAtMouse(mouseX, mouseY) {
      if (!this.heatmapCanvas) return null;
      const rect = this.heatmapCanvas.getBoundingClientRect();
      const dpr = window.devicePixelRatio || 1;
      let width = rect.width;
      let height = rect.height;
      if (width < 50 || height < 50) {
        width = this.heatmapCanvas.width / dpr || 460;
        height = this.heatmapCanvas.height / dpr || 460;
      }
      const margin = 32;
      const gridSize = Math.max(100, Math.min(width, height) - margin - 10);
      const cellSize = gridSize / 20;

      const offsetX = margin;
      const offsetY = margin;

      if (mouseX >= offsetX && mouseX <= offsetX + gridSize &&
          mouseY >= offsetY && mouseY <= offsetY + gridSize) {
        const col = Math.floor((mouseX - offsetX) / cellSize);
        const row = Math.floor((mouseY - offsetY) / cellSize);
        if (row >= 0 && row < 20 && col >= 0 && col < 20) {
          return [row, col];
        }
      }
      return null;
    }

    // =========================================================================
    // LOOP DE RENDERIZAÇÃO EM TEMPO REAL
    // =========================================================================
    startAnimationLoop() {
      const render = () => {
        if (this.isDestroyed) return;
        this.updateParticles();
        this.drawPoincareDisk();
        this.drawHeatmap();
        this.animFrameId = requestAnimationFrame(render);
      };
      this.animFrameId = requestAnimationFrame(render);
      this.drawHeatmap();
    }

    updateParticles() {
      for (const p of this.particles) {
        p.progress += p.speed;
        if (p.progress >= 1.0) {
          p.progress = 0;
          p.edgeIdx = Math.floor(Math.random() * this.edges.length);
        }
      }
    }

    /**
     * Renderização do Disco de Poincaré (Hyperbolic Network)
     */
    drawPoincareDisk() {
      if (!this.poincareCanvas || !this.poincareCtx) return;
      const canvas = this.poincareCanvas;
      const ctx = this.poincareCtx;
      const dpr = window.devicePixelRatio || 1;
      const rect = canvas.getBoundingClientRect();

      let width = rect.width;
      let height = rect.height;

      // Salvaguarda: se rect.width < 50 || rect.height < 50, use fallback
      if (width < 50 || height < 50) {
        if (canvas.parentElement && canvas.parentElement.clientWidth >= 50 && canvas.parentElement.clientHeight >= 50) {
          width = canvas.parentElement.clientWidth;
          height = canvas.parentElement.clientHeight;
        } else if (canvas.width >= 50 && canvas.height >= 50) {
          width = canvas.width / dpr;
          height = canvas.height / dpr;
        } else {
          width = 640;
          height = 460;
        }
      }

      if (canvas.width < 50 || canvas.height < 50) {
        canvas.width = Math.round(width * dpr);
        canvas.height = Math.round(height * dpr);
      }

      ctx.save();
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      ctx.scale(dpr, dpr);

      const cx = width / 2 + this.panOffset.x;
      const cy = height / 2 + this.panOffset.y;
      const rDisk = Math.min(width, height) * 0.44 * this.zoom;

      // 1. Círculo Fronteira do Disco de Poincaré (|z| = 1)
      ctx.beginPath();
      ctx.arc(cx, cy, rDisk, 0, 2 * Math.PI);
      ctx.fillStyle = '#06111C';
      ctx.fill();
      ctx.lineWidth = 2.5;
      ctx.strokeStyle = THEME.borderActive;
      ctx.shadowBlur = 18;
      ctx.shadowColor = THEME.kineNeon;
      ctx.stroke();
      ctx.shadowBlur = 0;

      // Círculos geodésicos concêntricos de referência (horociclos)
      const horocycles = [0.25, 0.50, 0.75];
      ctx.lineWidth = 1;
      ctx.strokeStyle = 'rgba(0, 167, 167, 0.15)';
      ctx.setLineDash([3, 4]);
      for (const h of horocycles) {
        ctx.beginPath();
        ctx.arc(cx, cy, rDisk * h, 0, 2 * Math.PI);
        ctx.stroke();
      }

      // Raios hiperbólicos de orientação (12 quadrantes)
      for (let a = 0; a < 12; a++) {
        const angle = (a * Math.PI) / 6;
        ctx.beginPath();
        ctx.moveTo(cx, cy);
        ctx.lineTo(cx + rDisk * Math.cos(angle), cy + rDisk * Math.sin(angle));
        ctx.stroke();
      }
      ctx.setLineDash([]);

      // 2. Renderização das 34 Arestas (Arcos Geodésicos de Poincaré)
      for (let i = 0; i < this.edges.length; i++) {
        const edge = this.edges[i];
        
        // Filtro de Curvatura
        if (this.filterMode === 'spherical' && edge.curvature < 0) continue;
        if (this.filterMode === 'hyperbolic' && edge.curvature > 0) continue;

        const isGeodesicEdge = this.isEdgeInGeodesicPath(edge.from, edge.to);
        this.renderHyperbolicArc(ctx, cx, cy, rDisk, edge, isGeodesicEdge);
      }

      // 3. Renderização de Partículas Energéticas sobre as arestas
      this.renderParticles(ctx, cx, cy, rDisk);

      // 4. Renderização dos 20 Nós Hiperbólicos
      for (let i = 0; i < this.nodes.length; i++) {
        const node = this.nodes[i];
        const nx = cx + node.r * rDisk * Math.cos(node.theta);
        const ny = cy + node.r * rDisk * Math.sin(node.theta);

        const isSelected = (i === this.selectedPair[0] || i === this.selectedPair[1]);
        const isPathNode = this.activeGeodesicPath.includes(i);
        const isHovered = (this.hoveredNode === i);

        this.renderNode(ctx, nx, ny, node, i, isSelected, isPathNode, isHovered);
      }

      ctx.restore();
    }

    /**
     * Desenha arco geodésico hiperbólico com gradiente de curvatura
     */
    renderHyperbolicArc(ctx, cx, cy, rDisk, edge, isHighlighted) {
      const n1 = this.nodes[edge.from];
      const n2 = this.nodes[edge.to];

      const u1 = n1.r * Math.cos(n1.theta);
      const v1 = n1.r * Math.sin(n1.theta);
      const u2 = n2.r * Math.cos(n2.theta);
      const v2 = n2.r * Math.sin(n2.theta);

      const x1 = cx + u1 * rDisk;
      const y1 = cy + v1 * rDisk;
      const x2 = cx + u2 * rDisk;
      const y2 = cy + v2 * rDisk;

      const arcInfo = computePoincareGeodesicArc(u1, v1, u2, v2);

      ctx.beginPath();
      if (arcInfo.isLine) {
        ctx.moveTo(x1, y1);
        ctx.lineTo(x2, y2);
      } else {
        const Cpx = cx + arcInfo.u0 * rDisk;
        const Cpy = cy + arcInfo.v0 * rDisk;
        const Rpx = arcInfo.rc * rDisk;
        ctx.arc(Cpx, Cpy, Rpx, arcInfo.a1, arcInfo.a2, arcInfo.anticlockwise);
      }

      // Estilo e espessura proporcionais à curvatura e destaque geodésico
      if (isHighlighted) {
        ctx.lineWidth = 4.5;
        ctx.strokeStyle = THEME.goldGlow;
        ctx.shadowBlur = 14;
        ctx.shadowColor = THEME.goldGlow;
        ctx.stroke();
        ctx.shadowBlur = 0;
      } else {
        ctx.lineWidth = Math.max(1.5, Math.min(4.0, 2.2 + edge.curvature * 25));
        ctx.strokeStyle = getCurvatureColor(edge.curvature, 0.75);
        ctx.stroke();
      }
    }

    /**
     * Renderiza partículas trafegando pelas geodésicas
     */
    renderParticles(ctx, cx, cy, rDisk) {
      for (const p of this.particles) {
        const edge = this.edges[p.edgeIdx];
        if (!edge) continue;

        const n1 = this.nodes[edge.from];
        const n2 = this.nodes[edge.to];
        const u1 = n1.r * Math.cos(n1.theta);
        const v1 = n1.r * Math.sin(n1.theta);
        const u2 = n2.r * Math.cos(n2.theta);
        const v2 = n2.r * Math.sin(n2.theta);

        const arcInfo = computePoincareGeodesicArc(u1, v1, u2, v2);

        let px, py;
        if (arcInfo.isLine) {
          px = cx + (u1 + (u2 - u1) * p.progress) * rDisk;
          py = cy + (v1 + (v2 - v1) * p.progress) * rDisk;
        } else {
          const Cpx = cx + arcInfo.u0 * rDisk;
          const Cpy = cy + arcInfo.v0 * rDisk;
          const Rpx = arcInfo.rc * rDisk;

          let delta = arcInfo.a2 - arcInfo.a1;
          if (arcInfo.anticlockwise) {
            while (delta > 0) delta -= 2 * Math.PI;
          } else {
            while (delta < 0) delta += 2 * Math.PI;
          }
          const curAngle = arcInfo.a1 + delta * p.progress;
          px = Cpx + Rpx * Math.cos(curAngle);
          py = Cpy + Rpx * Math.sin(curAngle);
        }

        ctx.beginPath();
        ctx.arc(px, py, p.size, 0, 2 * Math.PI);
        ctx.fillStyle = edge.curvature > 0 ? THEME.kineNeon : THEME.warning;
        ctx.shadowBlur = 6;
        ctx.shadowColor = ctx.fillStyle;
        ctx.fill();
        ctx.shadowBlur = 0;
      }
    }

    /**
     * Renderiza nó com halo luminescente e rótulos
     */
    renderNode(ctx, x, y, node, idx, isSelected, isPathNode, isHovered) {
      const roleColor = THEME.roles[node.role] || THEME.kineCyan;
      const baseRadius = 8;
      const radius = isSelected ? baseRadius + 4 : (isPathNode || isHovered ? baseRadius + 2 : baseRadius);

      // Halo Luminoso
      const haloGrad = ctx.createRadialGradient(x, y, radius * 0.4, x, y, radius * 2.8);
      haloGrad.addColorStop(0, roleColor);
      haloGrad.addColorStop(0.6, 'rgba(0, 240, 255, 0.25)');
      haloGrad.addColorStop(1, 'rgba(0, 240, 255, 0)');

      ctx.beginPath();
      ctx.arc(x, y, radius * 2.8, 0, 2 * Math.PI);
      ctx.fillStyle = haloGrad;
      ctx.fill();

      // Núcleo do Nó
      ctx.beginPath();
      ctx.arc(x, y, radius, 0, 2 * Math.PI);
      ctx.fillStyle = isSelected ? THEME.goldGlow : roleColor;
      ctx.shadowBlur = isSelected ? 18 : 8;
      ctx.shadowColor = isSelected ? THEME.goldGlow : roleColor;
      ctx.fill();
      ctx.shadowBlur = 0;

      // Anel Exterior
      ctx.lineWidth = 1.8;
      ctx.strokeStyle = '#FFFFFF';
      ctx.stroke();

      // Rótulos do Nó
      if (this.showLabels) {
        ctx.font = 'bold 9px "JetBrains Mono", monospace';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillStyle = '#FFFFFF';
        ctx.fillText(`N${idx + 1}`, x, y);

        // Subtítulo descritivo em hover ou seleção
        if (isSelected || isHovered) {
          ctx.font = '10px "Inter", sans-serif';
          ctx.fillStyle = THEME.kineNeon;
          ctx.shadowBlur = 4;
          ctx.shadowColor = '#000000';
          ctx.fillText(node.name.split(' (')[0], x, y - radius - 10);
          ctx.shadowBlur = 0;
        }
      }
    }

    isEdgeInGeodesicPath(u, v) {
      const path = this.activeGeodesicPath;
      for (let i = 0; i < path.length - 1; i++) {
        if ((path[i] === u && path[i + 1] === v) || (path[i] === v && path[i + 1] === u)) {
          return true;
        }
      }
      return false;
    }

    // =========================================================================
    // RENDERIZAÇÃO DO HEATMAP 20x20 DO TENSOR MÉTRICO
    // =========================================================================
    drawHeatmap() {
      if (!this.heatmapCanvas || !this.heatmapCtx) return;
      const canvas = this.heatmapCanvas;
      const ctx = this.heatmapCtx;
      const dpr = window.devicePixelRatio || 1;
      const rect = canvas.getBoundingClientRect();

      let width = rect.width;
      let height = rect.height;

      // Salvaguarda: se rect.width < 50 || rect.height < 50, use fallback
      if (width < 50 || height < 50) {
        if (canvas.parentElement && canvas.parentElement.clientWidth >= 50 && canvas.parentElement.clientHeight >= 50) {
          width = canvas.parentElement.clientWidth;
          height = canvas.parentElement.clientHeight;
        } else if (canvas.width >= 50 && canvas.height >= 50) {
          width = canvas.width / dpr;
          height = canvas.height / dpr;
        } else {
          width = 460;
          height = 460;
        }
      }

      if (canvas.width < 50 || canvas.height < 50) {
        canvas.width = Math.round(width * dpr);
        canvas.height = Math.round(height * dpr);
      }

      ctx.save();
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      ctx.scale(dpr, dpr);

      const margin = 32;
      const gridSize = Math.max(100, Math.min(width, height) - margin - 10);
      const cellSize = gridSize / 20;

      const offsetX = margin;
      const offsetY = margin;

      // 1. Cabeçalhos de Colunas (N1 a N20)
      ctx.font = 'bold 8px "JetBrains Mono", monospace';
      ctx.textAlign = 'center';
      ctx.textBaseline = 'bottom';
      for (let c = 0; c < 20; c++) {
        const x = offsetX + c * cellSize + cellSize / 2;
        ctx.fillStyle = (this.hoveredCell && this.hoveredCell[1] === c) ? THEME.kineNeon : THEME.slateMuted;
        ctx.fillText(`N${c + 1}`, x, offsetY - 4);
      }

      // 2. Cabeçalhos de Linhas (N1 a N20)
      ctx.textAlign = 'right';
      ctx.textBaseline = 'middle';
      for (let r = 0; r < 20; r++) {
        const y = offsetY + r * cellSize + cellSize / 2;
        ctx.fillStyle = (this.hoveredCell && this.hoveredCell[0] === r) ? THEME.kineNeon : THEME.slateMuted;
        ctx.fillText(`N${r + 1}`, offsetX - 6, y);
      }

      // 3. Grid 20x20 das Células g_ij
      for (let r = 0; r < 20; r++) {
        for (let c = 0; c < 20; c++) {
          const val = this.matrix[r][c] || 0.0;
          const x = offsetX + c * cellSize;
          const y = offsetY + r * cellSize;

          // Preenchimento com Gradiente Térmico
          ctx.fillStyle = getMetricThermalColor(val);
          ctx.fillRect(x, y, cellSize - 1, cellSize - 1);

          // Diagonal destacada g_ii
          if (r === c) {
            ctx.fillStyle = 'rgba(255, 255, 255, 0.25)';
            ctx.fillRect(x + 1, y + 1, cellSize - 3, cellSize - 3);
          }
        }
      }

      // 4. Destaque de Crosshair ao passar o mouse (Hover)
      if (this.hoveredCell) {
        const [hr, hc] = this.hoveredCell;
        
        // Faixa horizontal
        ctx.fillStyle = 'rgba(0, 240, 255, 0.12)';
        ctx.fillRect(offsetX, offsetY + hr * cellSize, gridSize, cellSize);

        // Faixa vertical
        ctx.fillRect(offsetX + hc * cellSize, offsetY, cellSize, gridSize);

        // Borda luminescente na célula alvo
        const cx = offsetX + hc * cellSize;
        const cy = offsetY + hr * cellSize;
        ctx.lineWidth = 2.0;
        ctx.strokeStyle = THEME.kineNeon;
        ctx.shadowBlur = 10;
        ctx.shadowColor = THEME.kineNeon;
        ctx.strokeRect(cx - 0.5, cy - 0.5, cellSize, cellSize);
        ctx.shadowBlur = 0;
      }

      // 5. Destaque da Célula Selecionada
      if (this.selectedPair) {
        const [sr, sc] = this.selectedPair;
        const sx = offsetX + sc * cellSize;
        const sy = offsetY + sr * cellSize;
        ctx.lineWidth = 2.0;
        ctx.strokeStyle = THEME.goldGlow;
        ctx.shadowBlur = 12;
        ctx.shadowColor = THEME.goldGlow;
        ctx.strokeRect(sx - 0.5, sy - 0.5, cellSize, cellSize);
        ctx.shadowBlur = 0;
      }

      ctx.restore();
    }

    /**
     * Atualização direta da matriz por chamada externa
     */
    updateHeatmap(matrix) {
      if (Array.isArray(matrix) && matrix.length === 20) {
        this.matrix = matrix;
        this.drawHeatmap();
      }
    }

    // =========================================================================
    // INTEGRAÇÃO COM A API /api/v1/geometry/tensor
    // =========================================================================
    async fetchTensorData() {
      try {
        const isProxy = window.location.port === '3000' && (window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1');
        const apiBase = isProxy ? 'http://localhost:8080' : '';

        const res = await fetch(`${apiBase}/api/v1/geometry/tensor`, { cache: 'no-store' });
        if (!res.ok) throw new Error(`Status HTTP ${res.status}`);
        const data = await res.json();

        // 1. Matriz explícita
        if (data.matrix && Array.isArray(data.matrix)) {
          this.matrix = data.matrix;
        }

        // 2. Arestas e curvaturas reais
        if (data.edges && Array.isArray(data.edges)) {
          for (const edgeDto of data.edges) {
            const localEdge = this.edges.find(e => (e.from === edgeDto.from && e.to === edgeDto.to));
            if (localEdge) {
              if (edgeDto.curvature !== undefined) localEdge.curvature = edgeDto.curvature;
              if (edgeDto.load !== undefined) localEdge.load = edgeDto.load;
              if (edgeDto.weight !== undefined) localEdge.weight = edgeDto.weight;
            }
          }
          if (!data.matrix) {
            this.initMatrix();
          }
        }

        if (data.avg_curvature !== undefined) this.avgCurvature = data.avg_curvature;
        if (data.volume_invariant !== undefined) this.volumeInvariant = data.volume_invariant;
        if (data.route_epoch !== undefined) this.routeEpoch = data.route_epoch;

        // Atualiza UI textual
        const elVol = document.getElementById('geo2d-vol-inv');
        if (elVol) elVol.innerText = this.volumeInvariant.toFixed(2);
        const elEpoch = document.getElementById('geo2d-epoch');
        if (elEpoch) elEpoch.innerText = `Epoch ${this.routeEpoch}`;

        this.drawHeatmap();
        this.syncExistingCurvatureChart();
      } catch (err) {
        // Fallback resiliente sem poluir console
        console.debug('[KinePlex2DGeometry] API tensor offline. Operando em regime de fallback numérico.', err.message);
      }
    }

    /**
     * Sincroniza e enriquece o gráfico de Curvatura Gaussiana no index.html
     */
    syncExistingCurvatureChart() {
      const chartEl = document.getElementById('chart-curvature-dist');
      if (!chartEl || !window.Chart) return;

      const chartInstance = Chart.getChart(chartEl);
      if (!chartInstance) return;

      // Gera curva gaussiana teórica enriquecida
      const mu = this.avgCurvature;
      const sigma = 0.018;
      const labels = [];
      const densityData = [];

      for (let x = -0.05; x <= 0.09; x += 0.005) {
        labels.push(x.toFixed(3));
        const y = (10 / (sigma * Math.sqrt(2 * Math.PI))) * Math.exp(-Math.pow(x - mu, 2) / (2 * Math.pow(sigma, 2))) * 0.05;
        densityData.push(parseFloat(y.toFixed(3)));
      }

      chartInstance.data.labels = labels;
      if (chartInstance.data.datasets && chartInstance.data.datasets.length > 0) {
        chartInstance.data.datasets[0].data = densityData;
        chartInstance.update();
      }
    }

    destroy() {
      this.isDestroyed = true;
      if (this.animFrameId) cancelAnimationFrame(this.animFrameId);
      if (this.pollInterval) clearInterval(this.pollInterval);
      if (this.resizeObserver) {
        this.resizeObserver.disconnect();
        this.resizeObserver = null;
      }
    }
  }

  // ===========================================================================
  // INTERFACES GLOBAIS EXPOSTAS
  // ===========================================================================

  window.KinePlex2DGeometry = KinePlex2DGeometry;

  window.resize2DGeometry = () => window.kineplex2DGeometry && window.kineplex2DGeometry.resize();

  window.init2DGeometry = function (containerId) {
    const targetId = containerId || 'kineplex-2d-geometry-container';
    if (window.kineplex2DGeometry) {
      if (window.kineplex2DGeometry.rootContainer && window.kineplex2DGeometry.rootContainer.children.length > 0) {
        window.kineplex2DGeometry.resize();
        return window.kineplex2DGeometry;
      }
    }
    const instance = new KinePlex2DGeometry({ containerId: targetId });
    instance.mount(targetId);
    window.kineplex2DGeometry = instance;
    return instance;
  };

  window.update2DHeatmap = function (matrix) {
    if (window.kineplex2DGeometry) {
      window.kineplex2DGeometry.updateHeatmap(matrix);
    }
  };

  window.highlightGeodesic2D = function (u, v) {
    if (window.kineplex2DGeometry) {
      window.kineplex2DGeometry.highlightGeodesic(u, v);
    }
  };

  // Auto-inicialização quando a página estiver carregada
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
      window.init2DGeometry();
    });
  } else {
    setTimeout(() => {
      window.init2DGeometry();
    }, 100);
  }

})(window, document);

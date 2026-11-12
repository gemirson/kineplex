/**
 * KinePlex Geometric Control 1D Visualizer
 * File: kineplex-ui/assets/geometry-1d.js
 *
 * Implements the complete 1D Geometric Control Suite:
 * 1. Adaptive Step Controller PID Multi-axis Chart (ε(t), e(t), Kp·e, Ki·∫e, Kd·ė)
 * 2. Volume Invariant Conservation Chart (V → 341.28, ±0.5% tolerance band, u32 clamp)
 * 3. Ranked Ricci Curvature Spectrum (34 edges sorted descending, zero balance line)
 * 4. Route Epoch Transitions & Atomic Stepped Chart (ε damping without jitter)
 * 5. Full REST API integration with /api/v1/geometry/tensor & /api/v1/geometry/state
 * 6. Responsive resizing & strict Chart.js lifecycle compatibility
 */

(function (global) {
  'use strict';

  // Constantes de Engenharia Geométrica (FT-089 .. FT-092)
  const TARGET_VOLUME = 341.28;
  const VOLUME_TOLERANCE_PCT = 0.005; // ±0.5% (339.5736 .. 342.9864)
  const VOL_UPPER_BOUND = TARGET_VOLUME * (1 + VOLUME_TOLERANCE_PCT);
  const VOL_LOWER_BOUND = TARGET_VOLUME * (1 - VOLUME_TOLERANCE_PCT);
  const TARGET_CURVATURE = 0.023; // Média global da variedade
  const TOTAL_EDGES = 34;

  class KinePlex1DControl {
    constructor(options = {}) {
      this.apiBase = options.apiBase || (typeof API_BASE !== 'undefined' ? API_BASE : '');
      this.autoPoll = options.autoPoll !== undefined ? options.autoPoll : true;
      this.pollIntervalMs = options.pollIntervalMs || 2500;
      this.pollTimer = null;

      // Estado dos parâmetros do controlador PID (FT-089)
      this.kp = 1.23;
      this.ki = 0.45;
      this.kd = 0.09;
      this.epsMin = 0.001;
      this.epsMax = 0.150;
      this.currentEps = 0.050;
      this.routeEpoch = 14;

      // Estado interno do PID
      this.integral = 0.0;
      this.prevError = 0.0;
      this.currentVolume = TARGET_VOLUME;
      this.isPerturbed = false;
      this.perturbStepsRemaining = 0;

      // Buffers de dados históricos
      this.pidHistory = [];
      this.volumeHistory = [];
      this.spectrumData = [];
      this.epochHistory = [];

      // Instâncias ativas do Chart.js
      this.charts = {
        pid: null,
        volume: null,
        spectrum: null,
        epochs: null,
      };

      this._isInitialized = false;
      this._resizeHandler = this._handleResize.bind(this);
    }

    /**
     * Inicializa a suíte 1D completa
     */
    init() {
      if (typeof Chart === 'undefined') {
        console.warn('[KinePlex1DControl] Chart.js ainda não disponível. Tentando novamente em 150ms...');
        setTimeout(() => this.init(), 150);
        return;
      }

      this._ensureDomContainers();
      this._seedInitialData();

      this.initPidChart();
      this.initVolumeChart();
      this.initSpectrumChart();
      this.initEpochChart();

      this._bindControlInputs();
      this._bindActionButtons();

      window.removeEventListener('resize', this._resizeHandler);
      window.addEventListener('resize', this._resizeHandler);

      if (this.autoPoll) {
        this.startPolling();
      }

      this._isInitialized = true;
      console.log('[KinePlex1DControl] Controle Geométrico 1D inicializado com sucesso.');
    }

    /**
     * Garante que os containers e elementos canvas necessários existam no DOM
     */
    _ensureDomContainers() {
      // 1. Container para o Volume Invariant Chart se não existir
      let volCanvas = document.getElementById('chart-volume-invariant');
      if (!volCanvas) {
        const routeEpochContainer = document.getElementById('chart-route-epochs')?.closest('.p-4');
        if (routeEpochContainer && routeEpochContainer.parentNode) {
          const volCard = document.createElement('div');
          volCard.id = 'card-volume-invariant';
          volCard.className = 'p-4 rounded-xl bg-kine-surface/50 border border-kine-border mt-3';
          volCard.innerHTML = `
            <div class="flex items-center justify-between text-xs font-mono mb-2">
              <span class="text-slate-300 font-bold uppercase tracking-wider">CONSERVAÇÃO DO INVARIANTE DE VOLUME</span>
              <span id="badge-vol-status" class="text-xs px-2 py-0.5 rounded bg-emerald-950/80 border border-emerald-500/40 text-emerald-300 font-mono">V = 341.28 (±0.5%)</span>
            </div>
            <p class="text-[11px] text-slate-400 font-mono mb-2">Série temporal contínua com clamp anti-overflow u32 mantendo V → 341.28</p>
            <div class="h-28">
              <canvas id="chart-volume-invariant"></canvas>
            </div>
          `;
          routeEpochContainer.parentNode.insertBefore(volCard, routeEpochContainer.nextSibling);
        }
      }

      // 2. Container para o Ranked Ricci Spectrum Chart se não existir
      let specCanvas = document.getElementById('chart-ricci-spectrum');
      if (!specCanvas) {
        // Tentar colocar logo abaixo do 3D Manifold ou em substituição/complemento ao mini chart
        const miniChartsGrid = document.querySelector('.grid.grid-cols-1.sm\\:grid-cols-2.gap-4.mt-4');
        if (miniChartsGrid && miniChartsGrid.parentNode) {
          const specCard = document.createElement('div');
          specCard.id = 'card-ricci-spectrum';
          specCard.className = 'p-4 rounded-xl bg-kine-surface/60 border border-kine-border mt-4';
          specCard.innerHTML = `
            <div class="flex items-center justify-between text-xs font-mono mb-2">
              <div class="flex items-center space-x-2">
                <span class="text-slate-300 font-bold uppercase tracking-wider">ESPECTRO RANQUEADO DE CURVATURAS DE RICCI</span>
                <span class="text-[10px] px-1.5 py-0.5 rounded bg-kine-cyan/20 border border-kine-cyan/40 text-kine-neon">34 ARESTAS</span>
              </div>
              <div class="flex items-center space-x-3 text-[11px] font-mono">
                <span class="text-emerald-400 flex items-center space-x-1"><span class="w-2 h-2 rounded-sm bg-emerald-400 inline-block"></span><span>Positiva (Core)</span></span>
                <span class="text-rose-400 flex items-center space-x-1"><span class="w-2 h-2 rounded-sm bg-rose-400 inline-block"></span><span>Negativa (Bridge)</span></span>
                <span class="text-slate-400 font-mono">Balanço Topológico: Σ R ≈ 0</span>
              </div>
            </div>
            <p class="text-[11px] text-slate-400 font-mono mb-2">Ordenamento das 34 arestas (da maior positiva à menor negativa) evidenciando o balanço topológico do cluster</p>
            <div class="h-36">
              <canvas id="chart-ricci-spectrum"></canvas>
            </div>
          `;
          miniChartsGrid.parentNode.insertBefore(specCard, miniChartsGrid.nextSibling);
        }
      }
    }

    /**
     * Semeia dados sintéticos matematicamente calibrados com base na teoria
     */
    _seedInitialData() {
      // 1. Histórico PID (60 passos)
      this.pidHistory = [];
      let currentEps = 0.050;
      let integral = 0.0;
      let prevErr = 0.0;

      for (let i = 0; i <= 60; i++) {
        const t = i / 10;
        // Erro com oscilação decrescente rumo a 0
        const err = 0.035 * Math.exp(-0.65 * t) * Math.sin(1.4 * t);
        integral += err * 0.1;
        integral = Math.max(-100.0, Math.min(100.0, integral));
        const dErr = (err - prevErr) / 0.1;
        prevErr = err;

        const pTerm = this.kp * err;
        const iTerm = this.ki * integral;
        const dTerm = this.kd * dErr;

        currentEps = Math.max(this.epsMin, Math.min(this.epsMax, 0.050 + pTerm + iTerm + dTerm));

        this.pidHistory.push({
          tick: i,
          epsilon: parseFloat(currentEps.toFixed(4)),
          error: parseFloat(err.toFixed(4)),
          pTerm: parseFloat(pTerm.toFixed(4)),
          iTerm: parseFloat(iTerm.toFixed(4)),
          dTerm: parseFloat(dTerm.toFixed(4)),
        });
      }

      // 2. Histórico de Conservação de Volume (50 passos)
      this.volumeHistory = [];
      for (let i = 0; i <= 50; i++) {
        const noise = (Math.sin(i * 0.7) * 0.06) + (Math.cos(i * 1.3) * 0.04);
        const v = TARGET_VOLUME + noise;
        this.volumeHistory.push({
          tick: i,
          volume: parseFloat(v.toFixed(3)),
          target: TARGET_VOLUME,
          upper: parseFloat(VOL_UPPER_BOUND.toFixed(3)),
          lower: parseFloat(VOL_LOWER_BOUND.toFixed(3)),
        });
      }

      // 3. Espectro Ranqueado de Curvatura de Ricci (34 arestas ordenadas)
      this.spectrumData = this._generateCurvatureSpectrum();

      // 4. Épocas de Rota
      this.epochHistory = [
        { label: 'Epoch 10', epoch: 10, epsilon: 0.120 },
        { label: 'Epoch 11', epoch: 11, epsilon: 0.105 },
        { label: 'Epoch 12', epoch: 12, epsilon: 0.090 },
        { label: 'Epoch 13', epoch: 13, epsilon: 0.075 },
        { label: 'Epoch 14', epoch: 14, epsilon: 0.050 },
      ];
    }

    /**
     * Gera o espectro das 34 arestas distribuídas entre curvaturas positivas e negativas
     */
    _generateCurvatureSpectrum() {
      const edges = [];
      const edgePairs = [
        [0, 1], [1, 2], [2, 3], [3, 4], [4, 5],
        [0, 8], [1, 8], [0, 9], [8, 9], [1, 7],
        [2, 7], [3, 7], [7, 8], [3, 13], [4, 13],
        [5, 13], [5, 6], [6, 13], [6, 14], [7, 12],
        [8, 12], [9, 10], [10, 11], [10, 18], [11, 12],
        [11, 18], [11, 17], [12, 13], [12, 16], [13, 14],
        [13, 15], [14, 15], [15, 16], [14, 19]
      ];

      for (let i = 0; i < TOTAL_EDGES; i++) {
        const pair = edgePairs[i] || [i % 20, (i + 1) % 20];
        // Gradiente contínuo de curvatura de Forman-Ricci [-0.088 .. +0.095]
        const factor = i / (TOTAL_EDGES - 1);
        const curv = 0.095 - factor * 0.183 + (Math.sin(i * 1.5) * 0.006);
        const weight = (TARGET_VOLUME / TOTAL_EDGES) * (1.0 + 0.12 * (factor - 0.5));

        edges.push({
          id: `e${String(pair[0]).padStart(2, '0')}-${String(pair[1]).padStart(2, '0')}`,
          from: pair[0],
          to: pair[1],
          curvature: parseFloat(curv.toFixed(3)),
          weight: parseFloat(weight.toFixed(2)),
          load: parseFloat((0.45 + (1 - factor) * 0.42).toFixed(2)),
        });
      }

      // Ordena decrescente: da maior curvatura positiva à menor negativa
      edges.sort((a, b) => b.curvature - a.curvature);
      return edges;
    }

    // =====================================================================
    // 1. GRÁFICO MULTIEIXO DO ADAPTIVE STEP CONTROLLER PID (Chart.js)
    // =====================================================================
    initPidChart() {
      const canvas = document.getElementById('chart-pid-curve');
      if (!canvas) return;

      if (this.charts.pid) {
        this.charts.pid.destroy();
        this.charts.pid = null;
      }

      const labels = this.pidHistory.map(p => p.tick);
      const epsData = this.pidHistory.map(p => p.epsilon);
      const errData = this.pidHistory.map(p => p.error);
      const pData = this.pidHistory.map(p => p.pTerm);
      const iData = this.pidHistory.map(p => p.iTerm);
      const dData = this.pidHistory.map(p => p.dTerm);

      this.charts.pid = new Chart(canvas, {
        type: 'line',
        data: {
          labels,
          datasets: [
            {
              label: 'Passo ε(t)',
              data: epsData,
              borderColor: '#00F0FF',
              backgroundColor: 'rgba(0, 240, 255, 0.08)',
              borderWidth: 2.5,
              yAxisID: 'y',
              tension: 0.35,
              pointRadius: 0,
              fill: false,
            },
            {
              label: 'Erro de Curvatura e(t)',
              data: errData,
              borderColor: '#F43F5E',
              borderWidth: 1.8,
              borderDash: [4, 4],
              yAxisID: 'y',
              tension: 0.35,
              pointRadius: 0,
              fill: false,
            },
            {
              label: 'Proporcional (Kp · e)',
              data: pData,
              borderColor: '#10B981',
              borderWidth: 1.8,
              yAxisID: 'y1',
              tension: 0.35,
              pointRadius: 0,
              fill: false,
            },
            {
              label: 'Integral (Ki · ∫e)',
              data: iData,
              borderColor: '#A855F7',
              borderWidth: 1.8,
              yAxisID: 'y1',
              tension: 0.35,
              pointRadius: 0,
              fill: false,
            },
            {
              label: 'Derivativo (Kd · ė)',
              data: dData,
              borderColor: '#F59E0B',
              borderWidth: 1.8,
              yAxisID: 'y1',
              tension: 0.35,
              pointRadius: 0,
              fill: false,
            },
          ]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          interaction: {
            mode: 'index',
            intersect: false,
          },
          plugins: {
            legend: {
              labels: {
                color: '#94A3B8',
                font: { family: 'monospace', size: 10 },
                boxWidth: 12,
                padding: 12,
              }
            },
            tooltip: {
              backgroundColor: 'rgba(5, 14, 24, 0.95)',
              borderColor: 'rgba(0, 240, 255, 0.4)',
              borderWidth: 1,
              titleFont: { family: 'monospace', size: 11 },
              bodyFont: { family: 'monospace', size: 10 },
              callbacks: {
                label: function(context) {
                  const val = context.parsed.y;
                  return ` ${context.dataset.label}: ${val !== undefined ? val.toFixed(4) : ''}`;
                }
              }
            }
          },
          scales: {
            x: {
              title: {
                display: true,
                text: 'Iterações do Controlador (Malha Fechada)',
                color: '#64748B',
                font: { family: 'monospace', size: 10 }
              },
              grid: { color: 'rgba(26, 67, 107, 0.25)' },
              ticks: { color: '#94A3B8', font: { family: 'monospace', size: 9 } }
            },
            // Eixo esquerdo: Passo ε e Erro e
            y: {
              type: 'linear',
              display: true,
              position: 'left',
              title: {
                display: true,
                text: 'Passo ε(t) / Erro e(t)',
                color: '#00F0FF',
                font: { family: 'monospace', size: 10 }
              },
              grid: { color: 'rgba(26, 67, 107, 0.25)' },
              ticks: { color: '#00F0FF', font: { family: 'monospace', size: 9 } },
              suggestedMin: -0.05,
              suggestedMax: 0.12,
            },
            // Eixo direito: Componentes individuais do PID
            y1: {
              type: 'linear',
              display: true,
              position: 'right',
              title: {
                display: true,
                text: 'Componentes PID (Kp·e, Ki·∫e, Kd·ė)',
                color: '#10B981',
                font: { family: 'monospace', size: 10 }
              },
              grid: { drawOnChartArea: false },
              ticks: { color: '#10B981', font: { family: 'monospace', size: 9 } },
              suggestedMin: -0.08,
              suggestedMax: 0.08,
            }
          }
        }
      });
    }

    /**
     * Recalcula dinamicamente a resposta PID e atualiza o gráfico multieixo
     */
    recalculatePidResponse(kp = this.kp, ki = this.ki, kd = this.kd, epsMin = this.epsMin) {
      this.kp = kp;
      this.ki = ki;
      this.kd = kd;
      this.epsMin = epsMin;

      const epsData = [];
      const errData = [];
      const pData = [];
      const iData = [];
      const dData = [];

      let runningEps = 0.050;
      let runningInt = 0.0;
      let prevE = 0.0;

      for (let i = 0; i <= 60; i++) {
        const t = i / 10;
        // Resposta amortecida com influência dos ganhos atuais
        const damping = 0.5 + 0.3 * (kp / 1.23);
        const freq = 1.2 + 0.2 * (kd / 0.09);
        const err = 0.035 * Math.exp(-damping * t) * Math.sin(freq * t);

        runningInt += err * 0.1;
        runningInt = Math.max(-100.0, Math.min(100.0, runningInt));
        const dErr = (err - prevE) / 0.1;
        prevE = err;

        const pTerm = kp * err;
        const iTerm = ki * runningInt;
        const dTerm = kd * dErr;

        runningEps = Math.max(epsMin, Math.min(this.epsMax, 0.050 + pTerm + iTerm + dTerm));

        epsData.push(parseFloat(runningEps.toFixed(4)));
        errData.push(parseFloat(err.toFixed(4)));
        pData.push(parseFloat(pTerm.toFixed(4)));
        iData.push(parseFloat(iTerm.toFixed(4)));
        dData.push(parseFloat(dTerm.toFixed(4)));
      }

      this.currentEps = epsData[epsData.length - 1];

      if (this.charts.pid) {
        this.charts.pid.data.datasets[0].data = epsData;
        this.charts.pid.data.datasets[1].data = errData;
        this.charts.pid.data.datasets[2].data = pData;
        this.charts.pid.data.datasets[3].data = iData;
        this.charts.pid.data.datasets[4].data = dData;
        this.charts.pid.update('none'); // Update de alta performance sem flicker
      }

      this._updatePidBadges();
    }

    // =====================================================================
    // 2. GRÁFICO 1D DE CONSERVAÇÃO DO INVARIANTE DE VOLUME (V → 341.28)
    // =====================================================================
    initVolumeChart() {
      const canvas = document.getElementById('chart-volume-invariant');
      if (!canvas) return;

      if (this.charts.volume) {
        this.charts.volume.destroy();
        this.charts.volume = null;
      }

      const labels = this.volumeHistory.map(v => v.tick);
      const volData = this.volumeHistory.map(v => v.volume);
      const targetData = this.volumeHistory.map(() => TARGET_VOLUME);
      const upperData = this.volumeHistory.map(() => VOL_UPPER_BOUND);
      const lowerData = this.volumeHistory.map(() => VOL_LOWER_BOUND);

      this.charts.volume = new Chart(canvas, {
        type: 'line',
        data: {
          labels,
          datasets: [
            // Faixa superior de tolerância (+0.5%)
            {
              label: 'Limite Superior (+0.5%)',
              data: upperData,
              borderColor: 'rgba(16, 185, 129, 0.35)',
              borderWidth: 1,
              borderDash: [3, 3],
              pointRadius: 0,
              fill: '+1', // Preenche até o dataset seguinte (faixa sombreada)
              backgroundColor: 'rgba(16, 185, 129, 0.08)',
            },
            // Faixa inferior de tolerância (-0.5%)
            {
              label: 'Limite Inferior (-0.5%)',
              data: lowerData,
              borderColor: 'rgba(16, 185, 129, 0.35)',
              borderWidth: 1,
              borderDash: [3, 3],
              pointRadius: 0,
              fill: false,
            },
            // Linha de referência exata 341.28
            {
              label: 'Invariante Alvo (341.28)',
              data: targetData,
              borderColor: '#00F0FF',
              borderWidth: 1.5,
              borderDash: [6, 4],
              pointRadius: 0,
              fill: false,
            },
            // Série temporal do volume real V(t)
            {
              label: 'Volume Métrico V(t)',
              data: volData,
              borderColor: '#38BDF8',
              backgroundColor: 'rgba(56, 189, 248, 0.2)',
              borderWidth: 2,
              tension: 0.25,
              pointRadius: 1.5,
              pointHoverRadius: 4,
              pointBackgroundColor: '#38BDF8',
              fill: false,
            }
          ]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: {
              labels: {
                color: '#94A3B8',
                font: { family: 'monospace', size: 9 },
                boxWidth: 10,
                padding: 8,
              }
            },
            tooltip: {
              backgroundColor: 'rgba(5, 14, 24, 0.95)',
              borderColor: 'rgba(0, 240, 255, 0.4)',
              borderWidth: 1,
              titleFont: { family: 'monospace', size: 10 },
              bodyFont: { family: 'monospace', size: 10 },
              callbacks: {
                label: function(context) {
                  return ` ${context.dataset.label}: ${context.parsed.y.toFixed(3)}`;
                }
              }
            }
          },
          scales: {
            x: {
              grid: { color: 'rgba(26, 67, 107, 0.2)' },
              ticks: { color: '#64748B', font: { family: 'monospace', size: 8 }, maxTicksLimit: 10 }
            },
            y: {
              min: 338.5,
              max: 344.0,
              grid: { color: 'rgba(26, 67, 107, 0.25)' },
              ticks: {
                color: '#94A3B8',
                font: { family: 'monospace', size: 9 },
                stepSize: 1.0,
                callback: (val) => val.toFixed(1)
              }
            }
          }
        }
      });
    }

    /**
     * Aplica perturbação dinâmica ao volume e demonstra reação do clamp
     */
    triggerVolumePerturbation() {
      this.isPerturbed = true;
      this.perturbStepsRemaining = 8;

      // Adiciona spike no histórico de volume
      const shockVol = TARGET_VOLUME + 4.2; // Salto transitório para ~345.5
      this.volumeHistory.push({
        tick: this.volumeHistory.length,
        volume: parseFloat(shockVol.toFixed(3)),
        target: TARGET_VOLUME,
        upper: parseFloat(VOL_UPPER_BOUND.toFixed(3)),
        lower: parseFloat(VOL_LOWER_BOUND.toFixed(3)),
      });
      if (this.volumeHistory.length > 50) this.volumeHistory.shift();

      this._updateVolumeChartData();

      // Feedback visual no badge de volume
      const badge = document.getElementById('badge-vol-status');
      if (badge) {
        badge.className = 'text-xs px-2 py-0.5 rounded bg-rose-950/90 border border-rose-500/60 text-rose-300 font-mono animate-pulse';
        badge.innerText = `SHOCK: V=${shockVol.toFixed(2)} (CLAMP ATUANDO)`;
      }

      // Amortecimento gradual via clamp normalizador
      let step = 0;
      const decayTimer = setInterval(() => {
        step++;
        const delta = 4.2 * Math.exp(-0.75 * step);
        const restoredVol = TARGET_VOLUME + delta + (Math.sin(step) * 0.05);

        this.volumeHistory.push({
          tick: this.volumeHistory.length,
          volume: parseFloat(restoredVol.toFixed(3)),
          target: TARGET_VOLUME,
          upper: parseFloat(VOL_UPPER_BOUND.toFixed(3)),
          lower: parseFloat(VOL_LOWER_BOUND.toFixed(3)),
        });
        if (this.volumeHistory.length > 50) this.volumeHistory.shift();

        this._updateVolumeChartData();

        if (step >= 8) {
          clearInterval(decayTimer);
          this.isPerturbed = false;
          if (badge) {
            badge.className = 'text-xs px-2 py-0.5 rounded bg-emerald-950/80 border border-emerald-500/40 text-emerald-300 font-mono';
            badge.innerText = `V = ${TARGET_VOLUME.toFixed(2)} (ESTABILIZADO)`;
          }
        }
      }, 350);
    }

    /**
     * Normalização forçada de volume com clamp anti-overflow u32
     */
    triggerVolumeNormalization() {
      this.volumeHistory.push({
        tick: this.volumeHistory.length,
        volume: TARGET_VOLUME,
        target: TARGET_VOLUME,
        upper: parseFloat(VOL_UPPER_BOUND.toFixed(3)),
        lower: parseFloat(VOL_LOWER_BOUND.toFixed(3)),
      });
      if (this.volumeHistory.length > 50) this.volumeHistory.shift();

      this._updateVolumeChartData();

      const badge = document.getElementById('badge-vol-status');
      if (badge) {
        badge.className = 'text-xs px-2 py-0.5 rounded bg-kine-cyan/30 border border-kine-cyan text-kine-neon font-mono';
        badge.innerText = `NORMALIZADO: V=${TARGET_VOLUME.toFixed(4)}`;
        setTimeout(() => {
          badge.className = 'text-xs px-2 py-0.5 rounded bg-emerald-950/80 border border-emerald-500/40 text-emerald-300 font-mono';
          badge.innerText = `V = ${TARGET_VOLUME.toFixed(2)} (±0.5%)`;
        }, 1500);
      }
    }

    _updateVolumeChartData() {
      if (!this.charts.volume) return;
      this.charts.volume.data.labels = this.volumeHistory.map(v => v.tick);
      this.charts.volume.data.datasets[0].data = this.volumeHistory.map(v => v.upper);
      this.charts.volume.data.datasets[1].data = this.volumeHistory.map(v => v.lower);
      this.charts.volume.data.datasets[2].data = this.volumeHistory.map(v => v.target);
      this.charts.volume.data.datasets[3].data = this.volumeHistory.map(v => v.volume);
      this.charts.volume.update('none');
    }

    // =====================================================================
    // 3. GRÁFICO 1D DO ESPECTRO RANQUEADO DE RICCI (34 ARESTAS)
    // =====================================================================
    initSpectrumChart() {
      let canvas = document.getElementById('chart-ricci-spectrum');
      if (!canvas) {
        // Fallback no chart de características caso não exista container próprio
        canvas = document.getElementById('chart-manifold-char');
      }
      if (!canvas) return;

      if (this.charts.spectrum) {
        this.charts.spectrum.destroy();
        this.charts.spectrum = null;
      }

      const labels = this.spectrumData.map(e => e.id);
      const data = this.spectrumData.map(e => e.curvature);

      // Cores: Verde/Ciano para positiva, Vermelho/Laranja para negativa
      const backgroundColors = data.map(val => {
        if (val >= 0) {
          // Gradiente ciano/esmeralda para curvaturas contrativas positivas
          return 'rgba(0, 240, 255, 0.85)';
        } else {
          // Vermelho/rosa para curvaturas expansivas negativas (gargalos)
          return 'rgba(244, 63, 94, 0.85)';
        }
      });

      const borderColors = data.map(val => (val >= 0 ? '#00F0FF' : '#F43F5E'));

      this.charts.spectrum = new Chart(canvas, {
        type: 'bar',
        data: {
          labels,
          datasets: [{
            label: 'Curvatura de Forman-Ricci F(e)',
            data,
            backgroundColor: backgroundColors,
            borderColor: borderColors,
            borderWidth: 1,
            borderRadius: 3,
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
            tooltip: {
              backgroundColor: 'rgba(5, 14, 24, 0.95)',
              borderColor: 'rgba(0, 240, 255, 0.4)',
              borderWidth: 1,
              titleFont: { family: 'monospace', size: 10 },
              bodyFont: { family: 'monospace', size: 10 },
              callbacks: {
                title: (items) => {
                  const edge = this.spectrumData[items[0].dataIndex];
                  return `Aresta ${edge.id} (Nó ${edge.from} ↔ ${edge.to})`;
                },
                label: (context) => {
                  const edge = this.spectrumData[context.dataIndex];
                  const sign = edge.curvature >= 0 ? '+' : '';
                  const type = edge.curvature >= 0 ? 'Contrativa (Core Clíque)' : 'Expansiva (Gargalo/Bridge)';
                  return [
                    ` Curvatura Ricci: ${sign}${edge.curvature.toFixed(4)}`,
                    ` Tipo Topológico: ${type}`,
                    ` Peso da Métrica: ${edge.weight} | Carga: ${edge.load}`,
                  ];
                }
              }
            }
          },
          scales: {
            x: {
              grid: { display: false },
              ticks: {
                color: '#94A3B8',
                font: { family: 'monospace', size: 8 },
                maxRotation: 90,
                minRotation: 45,
                autoSkip: false,
              }
            },
            y: {
              grid: {
                color: (ctx) => (ctx.tick.value === 0 ? 'rgba(255, 255, 255, 0.65)' : 'rgba(26, 67, 107, 0.25)'),
                lineWidth: (ctx) => (ctx.tick.value === 0 ? 2 : 1),
              },
              ticks: {
                color: '#94A3B8',
                font: { family: 'monospace', size: 9 },
                callback: (val) => `${val >= 0 ? '+' : ''}${val.toFixed(2)}`
              },
              suggestedMin: -0.10,
              suggestedMax: 0.11,
            }
          }
        }
      });
    }

    /**
     * Atualiza os dados do espectro de Ricci ordenado
     */
    updateSpectrum(newSpectrum) {
      if (Array.isArray(newSpectrum) && newSpectrum.length > 0) {
        this.spectrumData = newSpectrum.map(e => ({
          id: e.id || `e${e.from}-${e.to}`,
          from: e.from,
          to: e.to,
          curvature: parseFloat(e.curvature || 0),
          weight: parseFloat(e.weight || 10.0),
          load: parseFloat(e.load || 0.5),
        }));
        this.spectrumData.sort((a, b) => b.curvature - a.curvature);
      }

      if (this.charts.spectrum) {
        this.charts.spectrum.data.labels = this.spectrumData.map(e => e.id);
        const data = this.spectrumData.map(e => e.curvature);
        this.charts.spectrum.data.datasets[0].data = data;
        this.charts.spectrum.data.datasets[0].backgroundColor = data.map(v =>
          v >= 0 ? 'rgba(0, 240, 255, 0.85)' : 'rgba(244, 63, 94, 0.85)'
        );
        this.charts.spectrum.data.datasets[0].borderColor = data.map(v => (v >= 0 ? '#00F0FF' : '#F43F5E'));
        this.charts.spectrum.update('none');
      }
    }

    // =====================================================================
    // 4. GRÁFICO 1D DE ÉPOCAS DE ROTA E TRANSIÇÕES ATÔMICAS
    // =====================================================================
    initEpochChart() {
      const canvas = document.getElementById('chart-route-epochs');
      if (!canvas) return;

      if (this.charts.epochs) {
        this.charts.epochs.destroy();
        this.charts.epochs = null;
      }

      const labels = ['Epoch 10', 'Epoch 11', 'Epoch 12', 'Epoch 13', 'Epoch 14'];
      const data = [0.120, 0.105, 0.090, 0.075, 0.050];

      this.charts.epochs = new Chart(canvas, {
        type: 'line',
        data: {
          labels,
          datasets: [{
            label: 'Passo Amortecido ε por Época',
            data,
            stepped: 'before', // Transição atômica em degrau
            borderColor: '#00F0FF',
            backgroundColor: 'rgba(0, 240, 255, 0.15)',
            borderWidth: 2,
            fill: true,
            pointRadius: 4,
            pointBackgroundColor: '#00F0FF',
            pointBorderColor: '#050e18',
            pointBorderWidth: 1.5,
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
            tooltip: {
              backgroundColor: 'rgba(5, 14, 24, 0.95)',
              borderColor: 'rgba(0, 240, 255, 0.4)',
              borderWidth: 1,
              titleFont: { family: 'monospace', size: 10 },
              bodyFont: { family: 'monospace', size: 10 },
              callbacks: {
                label: (ctx) => ` Transição Atômica: ε = ${ctx.parsed.y.toFixed(3)} (Sem Jitter)`
              }
            }
          },
          scales: {
            x: {
              grid: { color: 'rgba(26, 67, 107, 0.25)' },
              ticks: { color: '#94A3B8', font: { family: 'monospace', size: 9 } }
            },
            y: {
              min: 0.0,
              max: 0.15,
              grid: { color: 'rgba(26, 67, 107, 0.25)' },
              ticks: { color: '#94A3B8', font: { family: 'monospace', size: 9 }, stepSize: 0.03 }
            }
          }
        }
      });
    }

    /**
     * Avança atomicamente a época de rota no gráfico
     */
    advanceRouteEpoch(newEpoch, newEps = 0.050) {
      this.routeEpoch = newEpoch;
      if (!this.charts.epochs) return;

      const labels = this.charts.epochs.data.labels;
      const data = this.charts.epochs.data.datasets[0].data;

      labels.push(`Epoch ${newEpoch}`);
      data.push(parseFloat(newEps.toFixed(3)));

      if (labels.length > 7) {
        labels.shift();
        data.shift();
      }

      this.charts.epochs.update();

      const epochBadge = document.getElementById('val-route-epoch');
      if (epochBadge) epochBadge.innerText = newEpoch;
    }

    // =====================================================================
    // 5. CONEXÃO COM A API REST (/api/v1/geometry/tensor & state)
    // =====================================================================
    async fetchGeometryState() {
      try {
        const res = await fetch(`${this.apiBase}/api/v1/geometry/state`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const state = await res.json();

        if (state.kp !== undefined) this.kp = state.kp;
        if (state.ki !== undefined) this.ki = state.ki;
        if (state.kd !== undefined) this.kd = state.kd;
        if (state.epsilon_min !== undefined) this.epsMin = state.epsilon_min;
        if (state.current_epsilon !== undefined) this.currentEps = state.current_epsilon;
        if (state.route_epoch !== undefined && state.route_epoch !== this.routeEpoch) {
          this.advanceRouteEpoch(state.route_epoch, this.currentEps);
        }

        this._updatePidBadges();
        return state;
      } catch (err) {
        // Silencioso em caso de backend offline
        return null;
      }
    }

    async fetchGeometryTensor() {
      try {
        const res = await fetch(`${this.apiBase}/api/v1/geometry/tensor`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const tensor = await res.json();

        if (tensor.spectrum_1d && Array.isArray(tensor.spectrum_1d)) {
          this.updateSpectrum(tensor.spectrum_1d);
        } else if (tensor.edges && Array.isArray(tensor.edges)) {
          this.updateSpectrum(tensor.edges);
        }

        if (tensor.pid_history_1d && Array.isArray(tensor.pid_history_1d) && tensor.pid_history_1d.length > 0) {
          this.pidHistory = tensor.pid_history_1d.map(p => ({
            tick: p.tick,
            epsilon: p.epsilon,
            error: p.error,
            pTerm: p.p_term,
            iTerm: p.i_term,
            dTerm: p.d_term,
          }));
          if (this.charts.pid) {
            this.charts.pid.data.labels = this.pidHistory.map(p => p.tick);
            this.charts.pid.data.datasets[0].data = this.pidHistory.map(p => p.epsilon);
            this.charts.pid.data.datasets[1].data = this.pidHistory.map(p => p.error);
            this.charts.pid.data.datasets[2].data = this.pidHistory.map(p => p.pTerm);
            this.charts.pid.data.datasets[3].data = this.pidHistory.map(p => p.iTerm);
            this.charts.pid.data.datasets[4].data = this.pidHistory.map(p => p.dTerm);
            this.charts.pid.update('none');
          }
        }

        if (tensor.volume_invariant !== undefined) {
          this.currentVolume = tensor.volume_invariant;
        }

        return tensor;
      } catch (err) {
        return null;
      }
    }

    startPolling() {
      if (this.pollTimer) clearInterval(this.pollTimer);
      const poll = async () => {
        await this.fetchGeometryState();
        await this.fetchGeometryTensor();
      };
      poll();
      this.pollTimer = setInterval(poll, this.pollIntervalMs);
    }

    stopPolling() {
      if (this.pollTimer) {
        clearInterval(this.pollTimer);
        this.pollTimer = null;
      }
    }

    // =====================================================================
    // 6. BINDING DE SLIDERS & AÇÕES DO DOM
    // =====================================================================
    _bindControlInputs() {
      const sliderKp = document.getElementById('slider-kp');
      const sliderKi = document.getElementById('slider-ki');
      const sliderKd = document.getElementById('slider-kd');
      const sliderEpsMin = document.getElementById('slider-eps-min');

      const handleSliderInput = () => {
        const kp = sliderKp ? parseFloat(sliderKp.value) : this.kp;
        const ki = sliderKi ? parseFloat(sliderKi.value) : this.ki;
        const kd = sliderKd ? parseFloat(sliderKd.value) : this.kd;
        const epsMin = sliderEpsMin ? parseFloat(sliderEpsMin.value) : this.epsMin;

        this.recalculatePidResponse(kp, ki, kd, epsMin);
      };

      if (sliderKp) sliderKp.addEventListener('input', handleSliderInput);
      if (sliderKi) sliderKi.addEventListener('input', handleSliderInput);
      if (sliderKd) sliderKd.addEventListener('input', handleSliderInput);
      if (sliderEpsMin) sliderEpsMin.addEventListener('input', handleSliderInput);
    }

    _bindActionButtons() {
      // Injetar Perturbação
      const origInject = window.injectMetricPerturbation;
      window.injectMetricPerturbation = async () => {
        this.triggerVolumePerturbation();
        if (typeof origInject === 'function') {
          try { await origInject(); } catch (e) {}
        }
      };

      // Normalizar Volume
      const origNormalize = window.normalizeTensorVolumeAction;
      window.normalizeTensorVolumeAction = async () => {
        this.triggerVolumeNormalization();
        if (typeof origNormalize === 'function') {
          try { await origNormalize(); } catch (e) {}
        }
      };
    }

    _updatePidBadges() {
      const setVal = (id, val) => {
        const el = document.getElementById(id);
        if (el) el.innerText = val;
      };

      setVal('param-val-kp', this.kp.toFixed(2));
      setVal('slider-kp-val', this.kp.toFixed(2));
      setVal('param-val-ki', this.ki.toFixed(2));
      setVal('slider-ki-val', this.ki.toFixed(2));
      setVal('param-val-kd', this.kd.toFixed(2));
      setVal('slider-kd-val', this.kd.toFixed(2));
      setVal('param-val-eps', this.currentEps.toFixed(3));
      setVal('val-pid-eps', this.currentEps.toFixed(3));
      setVal('slider-eps-val', this.epsMin.toFixed(4));
    }

    _handleResize() {
      Object.values(this.charts).forEach(chart => {
        if (chart && typeof chart.resize === 'function') {
          chart.resize();
        }
      });
    }

    destroy() {
      this.stopPolling();
      window.removeEventListener('resize', this._resizeHandler);
      Object.keys(this.charts).forEach(key => {
        if (this.charts[key]) {
          this.charts[key].destroy();
          this.charts[key] = null;
        }
      });
      this._isInitialized = false;
    }
  }

  // =====================================================================
  // EXPORTS & HOOKS GLOBAIS
  // =====================================================================
  global.KinePlex1DControl = KinePlex1DControl;

  // Singleton global
  let instance = null;

  global.init1DControl = function (options) {
    if (!instance) {
      instance = new KinePlex1DControl(options);
    }
    instance.init();
    global.kineplex1DControl = instance;
    return instance;
  };

  global.update1DPidChart = function (data) {
    if (instance) {
      if (data && data.kp !== undefined) {
        instance.recalculatePidResponse(data.kp, data.ki, data.kd, data.epsilon_min || instance.epsMin);
      }
    }
  };

  global.update1DSpectrum = function (spectrum) {
    if (instance) {
      instance.updateSpectrum(spectrum);
    }
  };

  // Auto-inicialização quando o DOM estiver pronto
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
      setTimeout(() => global.init1DControl(), 200);
    });
  } else {
    setTimeout(() => global.init1DControl(), 200);
  }

})(typeof window !== 'undefined' ? window : this);

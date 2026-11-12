/**
 * KinePlex 3D Manifold & Simplex Architect
 * assets/geometry-3d.js
 *
 * Visualização 3D completa e interativa do Manifold de Ricci com os 20 nós do cluster KinePlex.
 * Compatível estritamente com Three.js r128.
 */

(function () {
  'use strict';

  // Configuração padrão dos 20 nós do cluster em espaço hiperbólico
  const DEFAULT_NODES = [
    { id: "kpx-node-01", name: "Nó 1 (Semeador/Ingress A)", role: "Ingress", numa: "NUMA 0", cpu: 48, mem: 72, rx: "4.8 GB/s", status: "READY", pos: [-2.10, 1.15, 0.45], tensor: [1.06, 0.94, 1.02], curvature: 0.024 },
    { id: "kpx-node-02", name: "Nó 2 (Receptor Zero-Copy A)", role: "Ingress", numa: "NUMA 1", cpu: 55, mem: 68, rx: "12.4 GB/s", status: "READY", pos: [-1.45, 1.45, 0.20], tensor: [1.02, 1.01, 0.98], curvature: 0.018 },
    { id: "kpx-node-03", name: "Nó 3 (Compute Worker 1)", role: "Compute", numa: "NUMA 0", cpu: 32, mem: 81, rx: "8.1 GB/s", status: "READY", pos: [-0.60, 1.35, -0.38], tensor: [1.14, 0.88, 1.05], curvature: 0.041 },
    { id: "kpx-node-04", name: "Nó 4 (Compute Worker 2)", role: "Compute", numa: "NUMA 1", cpu: 60, mem: 75, rx: "9.6 GB/s", status: "READY", pos: [0.35, 1.25, -0.55], tensor: [1.09, 0.91, 1.03], curvature: 0.023 },
    { id: "kpx-node-05", name: "Nó 5 (Aggregation Master A)", role: "Aggregation", numa: "NUMA 0", cpu: 41, mem: 64, rx: "6.3 GB/s", status: "READY", pos: [1.25, 1.05, 0.12], tensor: [0.96, 1.05, 1.01], curvature: 0.035 },
    { id: "kpx-node-06", name: "Nó 6 (Terminal Parquet A)", role: "Parquet", numa: "NUMA 1", cpu: 39, mem: 58, rx: "5.1 GB/s", status: "READY", pos: [2.05, 0.70, 0.72], tensor: [0.93, 1.08, 0.98], curvature: 0.024 },
    { id: "kpx-node-07", name: "Nó 7 (Relay & Shunt A)", role: "Standby", numa: "NUMA 0", cpu: 28, mem: 49, rx: "3.2 GB/s", status: "READY", pos: [1.50, -0.10, -0.60], tensor: [1.01, 0.99, 1.00], curvature: -0.015 },
    { id: "kpx-node-08", name: "Nó 8 (Compute Worker 3)", role: "Compute", numa: "NUMA 1", cpu: 44, mem: 62, rx: "7.4 GB/s", status: "READY", pos: [-0.40, 0.45, -0.78], tensor: [1.07, 0.94, 1.02], curvature: 0.029 },
    { id: "kpx-node-09", name: "Nó 9 (Edge Gateway QUIC A)", role: "Ingress", numa: "NUMA 0", cpu: 35, mem: 53, rx: "11.2 GB/s", status: "READY", pos: [-1.75, 0.40, 0.18], tensor: [1.03, 0.97, 1.01], curvature: 0.022 },
    { id: "kpx-node-10", name: "Nó 10 (Hot Standby Spare A)", role: "Standby", numa: "NUMA 1", cpu: 12, mem: 28, rx: "0.5 GB/s", status: "IDLE", pos: [-2.25, -0.35, -0.85], tensor: [0.94, 1.06, 0.97], curvature: -0.042 },
    { id: "kpx-node-11", name: "Nó 11 (Semeador/Ingress B)", role: "Ingress", numa: "NUMA 2", cpu: 44, mem: 70, rx: "5.2 GB/s", status: "READY", pos: [-1.90, -1.15, 0.65], tensor: [1.05, 0.95, 1.02], curvature: 0.025 },
    { id: "kpx-node-12", name: "Nó 12 (Compute Worker 4)", role: "Compute", numa: "NUMA 2", cpu: 58, mem: 78, rx: "8.9 GB/s", status: "READY", pos: [-1.05, -0.65, -0.42], tensor: [1.11, 0.89, 1.04], curvature: 0.019 },
    { id: "kpx-node-13", name: "Nó 13 (Compute Worker 5)", role: "Compute", numa: "NUMA 3", cpu: 62, mem: 82, rx: "9.8 GB/s", status: "READY", pos: [-0.15, -0.55, -0.68], tensor: [1.15, 0.85, 1.06], curvature: 0.028 },
    { id: "kpx-node-14", name: "Nó 14 (Compute Worker 6)", role: "Compute", numa: "NUMA 2", cpu: 50, mem: 73, rx: "8.4 GB/s", status: "READY", pos: [0.75, -0.45, -0.32], tensor: [1.08, 0.92, 1.03], curvature: 0.022 },
    { id: "kpx-node-15", name: "Nó 15 (Aggregation Master B)", role: "Aggregation", numa: "NUMA 3", cpu: 46, mem: 69, rx: "7.1 GB/s", status: "READY", pos: [1.55, -0.75, 0.22], tensor: [0.98, 1.03, 1.01], curvature: 0.031 },
    { id: "kpx-node-16", name: "Nó 16 (Shuffle Manager)", role: "Aggregation", numa: "NUMA 2", cpu: 52, mem: 66, rx: "10.5 GB/s", status: "READY", pos: [0.95, -1.35, 0.58], tensor: [0.99, 1.01, 1.01], curvature: 0.020 },
    { id: "kpx-node-17", name: "Nó 17 (Terminal Parquet B)", role: "Parquet", numa: "NUMA 3", cpu: 41, mem: 60, rx: "5.8 GB/s", status: "READY", pos: [0.15, -1.65, 0.85], tensor: [0.92, 1.09, 0.98], curvature: 0.022 },
    { id: "kpx-node-18", name: "Nó 18 (Terminal Parquet C)", role: "Parquet", numa: "NUMA 2", cpu: 38, mem: 56, rx: "5.4 GB/s", status: "READY", pos: [-0.65, -1.55, 0.78], tensor: [0.95, 1.06, 0.99], curvature: 0.026 },
    { id: "kpx-node-19", name: "Nó 19 (Relay & Shunt B)", role: "Standby", numa: "NUMA 3", cpu: 30, mem: 45, rx: "4.1 GB/s", status: "READY", pos: [-1.35, -1.65, 0.15], tensor: [1.00, 1.00, 1.00], curvature: -0.018 },
    { id: "kpx-node-20", name: "Nó 20 (Hot Standby Spare B)", role: "Standby", numa: "NUMA 3", cpu: 10, mem: 25, rx: "0.4 GB/s", status: "IDLE", pos: [2.20, -1.15, -0.75], tensor: [0.91, 1.09, 0.97], curvature: -0.048 }
  ];

  // 34 Conexões Sinápticas reais do cluster cobrindo todos os 20 nós
  const DEFAULT_EDGES = [
    // Top tier: Ingress & Workers A (NUMA 0 / NUMA 1)
    { from: 0, to: 1, curvature: 0.024, load: 0.82 },
    { from: 1, to: 2, curvature: 0.018, load: 0.88 },
    { from: 2, to: 3, curvature: 0.041, load: 0.76 },
    { from: 3, to: 4, curvature: 0.023, load: 0.84 },
    { from: 4, to: 5, curvature: 0.035, load: 0.69 },

    // Local loops & gateway intra/inter NUMA 0 & 1
    { from: 0, to: 8, curvature: 0.022, load: 0.74 },
    { from: 1, to: 8, curvature: 0.015, load: 0.80 },
    { from: 0, to: 9, curvature: -0.048, load: 0.25 },
    { from: 8, to: 9, curvature: -0.039, load: 0.20 },
    { from: 1, to: 7, curvature: 0.027, load: 0.65 },
    { from: 2, to: 7, curvature: 0.031, load: 0.58 },
    { from: 3, to: 7, curvature: 0.022, load: 0.62 },
    { from: 7, to: 8, curvature: 0.029, load: 0.70 },

    // Middle tier cross-links (NUMA 0, 1, 2)
    { from: 3, to: 13, curvature: 0.025, load: 0.72 },
    { from: 4, to: 13, curvature: 0.032, load: 0.66 },
    { from: 5, to: 13, curvature: 0.021, load: 0.60 },
    { from: 5, to: 6, curvature: 0.024, load: 0.54 },
    { from: 6, to: 13, curvature: 0.030, load: 0.48 },
    { from: 6, to: 14, curvature: 0.022, load: 0.56 },

    // Inter-NUMA backbones
    { from: 7, to: 12, curvature: 0.014, load: 0.79 },
    { from: 8, to: 12, curvature: 0.026, load: 0.75 },
    { from: 9, to: 10, curvature: -0.042, load: 0.35 },

    // Lower tier: Ingress B & Workers (NUMA 2 / NUMA 3)
    { from: 10, to: 11, curvature: 0.025, load: 0.78 },
    { from: 10, to: 18, curvature: -0.031, load: 0.52 },
    { from: 11, to: 12, curvature: 0.018, load: 0.85 },
    { from: 11, to: 18, curvature: 0.031, load: 0.45 },
    { from: 11, to: 17, curvature: 0.022, load: 0.68 },
    { from: 12, to: 13, curvature: 0.016, load: 0.88 },
    { from: 12, to: 16, curvature: 0.024, load: 0.72 },

    // Bottom tier: Shuffles, Aggregation B, and Parquet Terminals
    { from: 13, to: 14, curvature: 0.023, load: 0.81 },
    { from: 13, to: 15, curvature: 0.017, load: 0.86 },
    { from: 14, to: 15, curvature: 0.021, load: 0.77 },
    { from: 15, to: 16, curvature: 0.020, load: 0.64 },
    { from: 14, to: 19, curvature: -0.036, load: 0.18 }
  ];

  // Cores por função do nó
  const ROLE_COLORS = {
    'Ingress': 0xA855F7,     // Neon Roxo (#A855F7)
    'Compute': 0x00F0FF,     // Ciano (#00F0FF)
    'Aggregation': 0xF59E0B, // Âmbar (#F59E0B)
    'Parquet': 0x10B981,     // Verde Esmeralda (#10B981)
    'Standby': 0x64748B,     // Cinza Slate (#64748B)
    'FAILED': 0xEF4444       // Vermelho Crimson (#EF4444)
  };

  /**
   * Converte valor de curvatura de Ricci para cor Three.js
   * R > 0: gradiente ciano -> esmeralda
   * R < 0: gradiente magenta -> laranja
   */
  function curvatureToColor(r) {
    if (r >= 0) {
      const t = Math.min(1, Math.max(0, r / 0.045));
      const colCyan = new THREE.Color(0x00F0FF);
      const colEmerald = new THREE.Color(0x10B981);
      return colCyan.lerp(colEmerald, t);
    } else {
      const t = Math.min(1, Math.max(0, -r / 0.050));
      const colMagenta = new THREE.Color(0xEC4899);
      const colOrange = new THREE.Color(0xF97316);
      return colMagenta.lerp(colOrange, t);
    }
  }

  /**
   * Descobre 2-simplexos (faces triangulares onde todos os 3 nós são mutuamente conectados)
   */
  function discoverSimplexTriangles(nodeCount, edges) {
    const adj = Array.from({ length: nodeCount }, () => new Set());
    edges.forEach(e => {
      if (e.from < nodeCount && e.to < nodeCount) {
        adj[e.from].add(e.to);
        adj[e.to].add(e.from);
      }
    });

    const triangles = [];
    for (let i = 0; i < nodeCount; i++) {
      for (let j = i + 1; j < nodeCount; j++) {
        if (!adj[i].has(j)) continue;
        for (let k = j + 1; k < nodeCount; k++) {
          if (adj[i].has(k) && adj[j].has(k)) {
            triangles.push([i, j, k]);
          }
        }
      }
    }
    return triangles;
  }

  class KinePlex3DManifold {
    constructor(containerId = 'threejs-manifold-container') {
      this.containerId = containerId;
      this.container = document.getElementById(containerId);
      if (!this.container) {
        console.warn(`[KinePlex 3D] Container #${containerId} não encontrado.`);
        return;
      }

      // Estado interno
      this.visMode = 'wireframe'; // 'wireframe' | 'faces' | 'tensor'
      this.nodesData = JSON.parse(JSON.stringify(DEFAULT_NODES));
      this.edgesData = JSON.parse(JSON.stringify(DEFAULT_EDGES));
      this.triangles = discoverSimplexTriangles(this.nodesData.length, this.edgesData);

      // Controle de interação e inércia
      this.isDragging = false;
      this.prevMouse = { x: 0, y: 0 };
      this.rotVelocity = { x: 0, y: 0 };
      this.damping = 0.94;
      this.targetCameraZ = 4.4;
      this.mouse = new THREE.Vector2(-999, -999);
      this.hoveredNodeIndex = null;
      this.isDestroyed = false;

      // Geometrias e meshes
      this.nodeObjects = [];
      this.edgeObjects = [];
      this.simplexMesh = null;
      this.simplexGeometry = null;
      this.tensorEllipsoids = [];

      this.initScene();
      this.initLighting();
      this.buildManifold();
      this.initInteraction();
      this.initTooltip();
      this.fetchBackendTensor();
      this.startRenderLoop();
    }

    initScene() {
      const width = this.container.clientWidth || 500;
      const height = this.container.clientHeight || 380;

      // 1. Cena
      this.scene = new THREE.Scene();

      // 2. Câmera Perspectiva
      this.camera = new THREE.PerspectiveCamera(46, width / height, 0.1, 100);
      this.camera.position.set(0, 0, this.targetCameraZ);

      // 3. Renderizador WebGL
      this.renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true, powerPreference: "high-performance" });
      this.renderer.setSize(width, height);
      this.renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
      this.renderer.toneMapping = THREE.ACESFilmicToneMapping;
      this.renderer.toneMappingExposure = 1.15;

      this.container.innerHTML = '';
      this.renderer.domElement.setAttribute('role', 'img');
      this.renderer.domElement.setAttribute('aria-label', 'Visualização 3D interativa da variedade Riemanniana de Ricci com 20 nós e faces de 2-simplexos');
      this.renderer.domElement.setAttribute('tabindex', '0');
      this.container.appendChild(this.renderer.domElement);
      this.renderer.domElement.style.outline = 'none';

      // Grupo principal para o Manifold (facilita rotação com inércia e escala de pulso)
      this.manifoldGroup = new THREE.Group();
      this.scene.add(this.manifoldGroup);

      // Subgrupos
      this.simplexGroup = new THREE.Group();
      this.edgesGroup = new THREE.Group();
      this.nodesGroup = new THREE.Group();
      this.tensorGroup = new THREE.Group();
      this.tensorGroup.visible = false; // Oculto por padrão no modo wireframe

      this.manifoldGroup.add(this.simplexGroup);
      this.manifoldGroup.add(this.edgesGroup);
      this.manifoldGroup.add(this.nodesGroup);
      this.manifoldGroup.add(this.tensorGroup);
    }

    initLighting() {
      // Luz ambiente suave tom marinho profundo
      const ambientLight = new THREE.AmbientLight(0x0f172a, 1.4);
      this.scene.add(ambientLight);

      // Luz direcional principal em neon ciano (Key light)
      const dirLight1 = new THREE.DirectionalLight(0x00F0FF, 1.6);
      dirLight1.position.set(4, 6, 7);
      this.scene.add(dirLight1);

      // Luz direcional secundária em roxo violeta (Rim / Fill light)
      const dirLight2 = new THREE.DirectionalLight(0xA855F7, 1.2);
      dirLight2.position.set(-6, -4, -5);
      this.scene.add(dirLight2);

      // Ponto de luz central sutil para realçar transparências internas dos simplexos
      const pointLight = new THREE.PointLight(0x00F0FF, 0.9, 12);
      pointLight.position.set(0, 0, 0);
      this.scene.add(pointLight);
    }

    buildManifold() {
      // 1. Nós do Cluster (20 nós reais)
      const coreSphereGeom = new THREE.SphereGeometry(0.092, 24, 24);
      const haloSphereGeom = new THREE.SphereGeometry(0.155, 16, 16);
      const hitSphereGeom = new THREE.SphereGeometry(0.22, 12, 12);
      const unitCylinderGeom = new THREE.CylinderGeometry(1, 1, 1, 8);
      const tensorBaseGeom = new THREE.SphereGeometry(0.18, 16, 12);

      this.nodeObjects = this.nodesData.map((nodeData, idx) => {
        const nodeRoleColor = (nodeData.status === 'FAILED') 
          ? ROLE_COLORS['FAILED'] 
          : (ROLE_COLORS[nodeData.role] || 0x00F0FF);

        const basePos = new THREE.Vector3(...nodeData.pos);
        const currentPos = basePos.clone();
        const velocity = new THREE.Vector3(0, 0, 0);

        const nodeContainer = new THREE.Group();
        nodeContainer.position.copy(currentPos);

        // Esfera núcleo brilhante
        const coreMaterial = new THREE.MeshStandardMaterial({
          color: nodeRoleColor,
          emissive: nodeRoleColor,
          emissiveIntensity: 0.85,
          roughness: 0.15,
          metalness: 0.85
        });
        const coreMesh = new THREE.Mesh(coreSphereGeom, coreMaterial);
        nodeContainer.add(coreMesh);

        // Halo pulsante 3D (blending aditivo)
        const haloMaterial = new THREE.MeshBasicMaterial({
          color: nodeRoleColor,
          transparent: true,
          opacity: 0.28,
          blending: THREE.AdditiveBlending,
          side: THREE.BackSide,
          depthWrite: false
        });
        const haloMesh = new THREE.Mesh(haloSphereGeom, haloMaterial);
        nodeContainer.add(haloMesh);

        // Esfera de detecção de clique/hover ampliada (invisível)
        const hitMaterial = new THREE.MeshBasicMaterial({ visible: false });
        const hitMesh = new THREE.Mesh(hitSphereGeom, hitMaterial);
        hitMesh.userData = { nodeIndex: idx, nodeData };
        nodeContainer.add(hitMesh);

        this.nodesGroup.add(nodeContainer);

        // Elipsoide métrico de deformação tensor g_ii
        const tensorMat = new THREE.MeshStandardMaterial({
          color: 0x00F0FF,
          wireframe: true,
          transparent: true,
          opacity: 0.75,
          emissive: 0x00F0FF,
          emissiveIntensity: 0.4
        });
        const tensorMesh = new THREE.Mesh(tensorBaseGeom, tensorMat);
        tensorMesh.position.copy(currentPos);
        const [gxx, gyy, gzz] = nodeData.tensor || [1.0, 1.0, 1.0];
        tensorMesh.scale.set(gxx, gyy, gzz);

        // Eixos de deformação métrica principais
        const axesHelper = new THREE.AxesHelper(0.24);
        tensorMesh.add(axesHelper);

        this.tensorGroup.add(tensorMesh);

        return {
          idx,
          data: nodeData,
          basePos,
          currentPos,
          velocity,
          nodeContainer,
          coreMesh,
          haloMesh,
          hitMesh,
          tensorMesh,
          color: nodeRoleColor
        };
      });

      // 2. Conexões Sinápticas 3D (34 conexões)
      this.edgeObjects = this.edgesData.map(edgeData => {
        const pFrom = this.nodeObjects[edgeData.from].currentPos;
        const pTo = this.nodeObjects[edgeData.to].currentPos;

        const edgeColor = curvatureToColor(edgeData.curvature);
        const edgeMaterial = new THREE.MeshStandardMaterial({
          color: edgeColor,
          emissive: edgeColor,
          emissiveIntensity: 0.55,
          roughness: 0.25,
          metalness: 0.35,
          transparent: true,
          opacity: 0.88
        });

        const cylinderMesh = new THREE.Mesh(unitCylinderGeom, edgeMaterial);
        this.orientCylinder(cylinderMesh, pFrom, pTo, 0.022);
        this.edgesGroup.add(cylinderMesh);

        return {
          data: edgeData,
          from: edgeData.from,
          to: edgeData.to,
          cylinderMesh,
          material: edgeMaterial,
          baseColor: edgeColor
        };
      });

      // 3. 2-Simplexos 3D (Superfícies triangulares translúcidas com DoubleSide)
      this.buildSimplexMesh();
    }

    buildSimplexMesh() {
      if (this.simplexMesh) {
        this.simplexGroup.remove(this.simplexMesh);
        if (this.simplexGeometry) this.simplexGeometry.dispose();
      }

      const triCount = this.triangles.length;
      if (triCount === 0) return;

      const positions = new Float32Array(triCount * 9);
      const colors = new Float32Array(triCount * 9);

      let pIdx = 0;
      let cIdx = 0;

      this.triangles.forEach(([u, v, w]) => {
        const nodeU = this.nodeObjects[u];
        const nodeV = this.nodeObjects[v];
        const nodeW = this.nodeObjects[w];

        const pu = nodeU.currentPos;
        const pv = nodeV.currentPos;
        const pw = nodeW.currentPos;

        // Vértices
        positions[pIdx++] = pu.x; positions[pIdx++] = pu.y; positions[pIdx++] = pu.z;
        positions[pIdx++] = pv.x; positions[pIdx++] = pv.y; positions[pIdx++] = pv.z;
        positions[pIdx++] = pw.x; positions[pIdx++] = pw.y; positions[pIdx++] = pw.z;

        // Cores de Curvatura interpoladas
        const colU = curvatureToColor(nodeU.data.curvature);
        const colV = curvatureToColor(nodeV.data.curvature);
        const colW = curvatureToColor(nodeW.data.curvature);

        colors[cIdx++] = colU.r; colors[cIdx++] = colU.g; colors[cIdx++] = colU.b;
        colors[cIdx++] = colV.r; colors[cIdx++] = colV.g; colors[cIdx++] = colV.b;
        colors[cIdx++] = colW.r; colors[cIdx++] = colW.g; colors[cIdx++] = colW.b;
      });

      this.simplexGeometry = new THREE.BufferGeometry();
      this.simplexGeometry.setAttribute('position', new THREE.BufferAttribute(positions, 3));
      this.simplexGeometry.setAttribute('color', new THREE.BufferAttribute(colors, 3));
      this.simplexGeometry.computeVertexNormals();

      this.simplexMaterial = new THREE.MeshStandardMaterial({
        vertexColors: true,
        transparent: true,
        opacity: (this.visMode === 'faces') ? 0.65 : 0.16,
        roughness: 0.15,
        metalness: 0.10,
        side: THREE.DoubleSide,
        depthWrite: false
      });

      this.simplexMesh = new THREE.Mesh(this.simplexGeometry, this.simplexMaterial);
      this.simplexGroup.add(this.simplexMesh);
    }

    orientCylinder(cylinderMesh, pFrom, pTo, radius = 0.022) {
      const dir = new THREE.Vector3().subVectors(pTo, pFrom);
      const length = dir.length();
      if (length < 0.0001) return;

      const mid = new THREE.Vector3().addVectors(pFrom, pTo).multiplyScalar(0.5);
      cylinderMesh.position.copy(mid);
      cylinderMesh.scale.set(radius, length, radius);

      const up = new THREE.Vector3(0, 1, 0);
      cylinderMesh.quaternion.setFromUnitVectors(up, dir.clone().normalize());
    }

    updateDeformedPositions() {
      // 1. Atualiza conexões
      this.edgeObjects.forEach(edgeObj => {
        const pFrom = this.nodeObjects[edgeObj.from].currentPos;
        const pTo = this.nodeObjects[edgeObj.to].currentPos;
        this.orientCylinder(edgeObj.cylinderMesh, pFrom, pTo, 0.022);
      });

      // 2. Atualiza posições dos elipsoides
      this.nodeObjects.forEach(nodeObj => {
        nodeObj.tensorMesh.position.copy(nodeObj.currentPos);
      });

      // 3. Atualiza vértices dos 2-simplexos
      if (this.simplexGeometry) {
        const posAttr = this.simplexGeometry.getAttribute('position');
        const posArray = posAttr.array;
        let pIdx = 0;

        this.triangles.forEach(([u, v, w]) => {
          const pu = this.nodeObjects[u].currentPos;
          const pv = this.nodeObjects[v].currentPos;
          const pw = this.nodeObjects[w].currentPos;

          posArray[pIdx++] = pu.x; posArray[pIdx++] = pu.y; posArray[pIdx++] = pu.z;
          posArray[pIdx++] = pv.x; posArray[pIdx++] = pv.y; posArray[pIdx++] = pv.z;
          posArray[pIdx++] = pw.x; posArray[pIdx++] = pw.y; posArray[pIdx++] = pw.z;
        });

        posAttr.needsUpdate = true;
        this.simplexGeometry.computeVertexNormals();
      }
    }

    initInteraction() {
      this.raycaster = new THREE.Raycaster();

      // Arrastar para rotacionar com inércia
      this.container.addEventListener('pointerdown', (e) => {
        this.isDragging = true;
        this.prevMouse.x = e.clientX;
        this.prevMouse.y = e.clientY;
        this.container.style.cursor = 'grabbing';
      });

      window.addEventListener('pointerup', () => {
        if (this.isDragging) {
          this.isDragging = false;
          if (this.container) this.container.style.cursor = 'grab';
        }
      });

      this.container.addEventListener('pointermove', (e) => {
        const rect = this.container.getBoundingClientRect();
        this.mouse.x = ((e.clientX - rect.left) / rect.width) * 2 - 1;
        this.mouse.y = -((e.clientY - rect.top) / rect.height) * 2 + 1;

        if (this.isDragging) {
          const deltaX = e.clientX - this.prevMouse.x;
          const deltaY = e.clientY - this.prevMouse.y;
          this.rotVelocity.y = deltaX * 0.006;
          this.rotVelocity.x = deltaY * 0.006;
          this.manifoldGroup.rotation.y += this.rotVelocity.y;
          this.manifoldGroup.rotation.x += this.rotVelocity.x;
          this.prevMouse.x = e.clientX;
          this.prevMouse.y = e.clientY;
        }
      });

      // Zoom suave com clamp
      this.container.addEventListener('wheel', (e) => {
        e.preventDefault();
        this.targetCameraZ += e.deltaY * 0.003;
        this.targetCameraZ = Math.min(Math.max(this.targetCameraZ, 1.8), 7.5);
      }, { passive: false });

      // Suporte a Redimensionamento Responsivo
      window.addEventListener('resize', () => this.resize());
      if (window.ResizeObserver) {
        this.resizeObserver = new ResizeObserver(() => this.resize());
        this.resizeObserver.observe(this.container);
      }
    }

    initTooltip() {
      this.tooltip = document.createElement('div');
      this.tooltip.className = 'kineplex-3d-tooltip pointer-events-none absolute z-50 hidden transition-opacity duration-150';
      this.tooltip.style.cssText = `
        position: absolute;
        pointer-events: none;
        z-index: 50;
        display: none;
        background: rgba(5, 14, 24, 0.94);
        border: 1px solid rgba(0, 240, 255, 0.55);
        border-radius: 10px;
        padding: 10px 14px;
        box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.8), 0 0 16px rgba(0, 240, 255, 0.25);
        backdrop-filter: blur(8px);
        font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
        font-size: 11px;
        color: #e2e8f0;
        max-width: 290px;
        transform: translate(-50%, -125%);
      `;
      this.container.appendChild(this.tooltip);
    }

    checkRaycasting() {
      if (!this.raycaster || !this.camera || this.isDragging) return;

      this.raycaster.setFromCamera(this.mouse, this.camera);
      const hitMeshes = this.nodeObjects.map(n => n.hitMesh);
      const intersects = this.raycaster.intersectObjects(hitMeshes, false);

      if (intersects.length > 0) {
        const hit = intersects[0];
        const idx = hit.object.userData.nodeIndex;
        const nodeObj = this.nodeObjects[idx];

        if (this.hoveredNodeIndex !== idx) {
          // Restaura nó anterior
          if (this.hoveredNodeIndex !== null) {
            const prev = this.nodeObjects[this.hoveredNodeIndex];
            prev.nodeContainer.scale.set(1.0, 1.0, 1.0);
            prev.haloMesh.material.opacity = 0.28;
          }

          this.hoveredNodeIndex = idx;
          nodeObj.nodeContainer.scale.set(1.4, 1.4, 1.4);
          nodeObj.haloMesh.material.opacity = 0.75;
        }

        // Atualiza tooltip
        this.renderTooltipContent(nodeObj);
      } else {
        if (this.hoveredNodeIndex !== null) {
          const prev = this.nodeObjects[this.hoveredNodeIndex];
          prev.nodeContainer.scale.set(1.0, 1.0, 1.0);
          prev.haloMesh.material.opacity = 0.28;
          this.hoveredNodeIndex = null;
        }
        if (this.tooltip) {
          this.tooltip.style.display = 'none';
        }
      }
    }

    renderTooltipContent(nodeObj) {
      if (!this.tooltip) return;
      const d = nodeObj.data;
      const rect = this.container.getBoundingClientRect();

      // Projeta coordenadas 3D para posição no canvas
      const screenPos = nodeObj.currentPos.clone().applyMatrix4(this.manifoldGroup.matrixWorld).project(this.camera);
      const x = (screenPos.x * 0.5 + 0.5) * rect.width;
      const y = (-(screenPos.y * 0.5) + 0.5) * rect.height;

      const isPos = d.curvature >= 0;
      const curvStr = `${isPos ? '+' : ''}${d.curvature.toFixed(3)}`;
      const curvColor = isPos ? 'text-emerald-400' : 'text-rose-400';
      const [gxx, gyy, gzz] = d.tensor || [1.0, 1.0, 1.0];

      const roleBadgeClass = {
        'Ingress': 'bg-purple-500/20 text-purple-300 border-purple-500/40',
        'Compute': 'bg-cyan-500/20 text-cyan-300 border-cyan-500/40',
        'Aggregation': 'bg-amber-500/20 text-amber-300 border-amber-500/40',
        'Parquet': 'bg-emerald-500/20 text-emerald-300 border-emerald-500/40',
        'Standby': 'bg-slate-500/20 text-slate-300 border-slate-500/40'
      }[d.role] || 'bg-slate-500/20 text-slate-300 border-slate-500/40';

      this.tooltip.innerHTML = `
        <div class="flex items-center justify-between pb-1.5 border-b border-slate-700/80 mb-2">
          <div class="flex items-center space-x-1.5">
            <span class="w-2 h-2 rounded-full ${d.status === 'READY' ? 'bg-emerald-400' : (d.status === 'IDLE' ? 'bg-amber-400' : 'bg-rose-500 animate-ping')}"></span>
            <span class="font-bold text-white tracking-wide">${d.id}</span>
          </div>
          <span class="px-1.5 py-0.5 rounded border text-[10px] font-mono ${roleBadgeClass}">${d.role.toUpperCase()}</span>
        </div>
        <div class="space-y-1 text-[11px] leading-tight">
          <div class="text-slate-300 font-medium truncate">${d.name}</div>
          <div class="flex justify-between text-slate-400">
            <span>Domínio:</span> <span class="text-amber-400 font-semibold">${d.numa}</span>
          </div>
          <div class="flex justify-between text-slate-400">
            <span>Curvatura Ricci:</span> <span class="font-bold font-mono ${curvColor}">${curvStr}</span>
          </div>
          <div class="flex justify-between text-slate-400">
            <span>Tensor Métrico g_ii:</span> <span class="text-cyan-300 font-mono">[${gxx.toFixed(2)}, ${gyy.toFixed(2)}, ${gzz.toFixed(2)}]</span>
          </div>
          <div class="flex justify-between text-slate-400">
            <span>Throughput RX:</span> <span class="text-emerald-400 font-mono">${d.rx}</span>
          </div>
          <div class="flex justify-between text-slate-400">
            <span>CPU / Mem:</span> <span class="text-slate-200 font-mono">${d.cpu}% / ${d.mem}%</span>
          </div>
        </div>
      `;

      this.tooltip.style.left = `${Math.max(120, Math.min(rect.width - 120, x))}px`;
      this.tooltip.style.top = `${Math.max(70, Math.min(rect.height - 20, y))}px`;
      this.tooltip.style.display = 'block';
    }

    /**
     * Animação do Pulso de Ricci (morphManifoldShallow)
     * Aplica uma onda harmônica de deformação espacial e relaxa via física de amortecimento elástico
     */
    morphManifoldShallow() {
      const pulseMagnitude = 0.24;
      const time = performance.now() * 0.001;

      this.nodeObjects.forEach((nodeObj, idx) => {
        const wave = Math.sin(time * 3.5 + idx * 0.75) * pulseMagnitude;
        const dir = nodeObj.basePos.clone().normalize();
        if (dir.lengthSq() < 0.0001) dir.set(0, 1, 0);

        // Perturba a velocidade para ativar oscilação elástica amortecida
        nodeObj.velocity.add(dir.multiplyScalar(wave * 1.6));

        // Flash e pulso no halo
        if (nodeObj.haloMesh) {
          nodeObj.haloMesh.material.opacity = 0.90;
          nodeObj.haloMesh.scale.setScalar(2.1);
        }
      });

      // Pulso luminoso de excitação em todas as arestas sinápticas
      this.edgeObjects.forEach(edgeObj => {
        edgeObj.material.emissiveIntensity = 1.2;
      });
    }

    /**
     * Modos de Visualização 3D
     * wireframe: Malha hiperbólica com ênfase nas arestas e nós
     * faces: Destaque nas faces 2-simplex translúcidas com mapa de calor de curvatura
     * tensor: Elipsoides de deformação métrica posicionados nos 20 nós refletindo g_ii
     */
    changeVisMode(mode) {
      this.visMode = mode;

      if (mode === 'faces') {
        if (this.simplexMaterial) this.simplexMaterial.opacity = 0.65;
        this.edgeObjects.forEach(e => { e.material.opacity = 0.40; });
        this.tensorGroup.visible = false;
      } else if (mode === 'tensor') {
        if (this.simplexMaterial) this.simplexMaterial.opacity = 0.18;
        this.edgeObjects.forEach(e => { e.material.opacity = 0.55; });
        this.tensorGroup.visible = true;
      } else { // 'wireframe' padrão
        if (this.simplexMaterial) this.simplexMaterial.opacity = 0.14;
        this.edgeObjects.forEach(e => { e.material.opacity = 0.88; });
        this.tensorGroup.visible = false;
      }
    }

    /**
     * Reset suave da Câmera e Orientação
     */
    resetCamera() {
      this.targetCameraZ = 4.4;
      this.rotVelocity.x = 0;
      this.rotVelocity.y = 0;
      this.manifoldGroup.rotation.set(0, 0, 0);
      this.camera.position.set(0, 0, 4.4);
    }

    /**
     * Responsividade ao Redimensionamento
     */
    resize() {
      if (!this.container || !this.renderer || !this.camera) return;
      const width = this.container.clientWidth || 500;
      const height = this.container.clientHeight || 380;
      this.camera.aspect = width / height;
      this.camera.updateProjectionMatrix();
      this.renderer.setSize(width, height);
    }

    /**
     * Sincronização graciosa com nós atualizados do cluster
     */
    syncWithClusterNodes(remoteNodes) {
      if (!Array.isArray(remoteNodes) || remoteNodes.length === 0) return;

      remoteNodes.forEach(rn => {
        const local = this.nodeObjects.find(n => n.data.id === rn.id);
        if (local) {
          local.data.status = rn.status;
          local.data.cpu = rn.cpu;
          local.data.mem = rn.mem;
          local.data.rx = rn.rx;

          // Atualiza cor em caso de falha (chaos)
          const newColor = (rn.status === 'FAILED') 
            ? ROLE_COLORS['FAILED'] 
            : (ROLE_COLORS[local.data.role] || 0x00F0FF);

          local.coreMesh.material.color.setHex(newColor);
          local.coreMesh.material.emissive.setHex(newColor);
          local.haloMesh.material.color.setHex(newColor);
        }
      });
    }

    /**
     * Tenta buscar coordenadas e tensores de /api/v1/geometry/tensor com fallback gracioso
     */
    async fetchBackendTensor() {
      try {
        const apiBase = (typeof API_BASE !== 'undefined') ? API_BASE : '';
        const res = await fetch(`${apiBase}/api/v1/geometry/tensor`);
        if (res.ok) {
          const payload = await res.json();
          if (payload) {
            // Suporte ao formato nodes_3d (MetricTensorResponseDto)
            if (Array.isArray(payload.nodes_3d)) {
              payload.nodes_3d.forEach((rn, i) => {
                if (this.nodeObjects[i]) {
                  if (typeof rn.x === 'number' && typeof rn.y === 'number' && typeof rn.z === 'number') {
                    this.nodeObjects[i].basePos.set(rn.x, rn.y, rn.z);
                  }
                  if (rn.role) this.nodeObjects[i].data.role = rn.role;
                  if (rn.numa) this.nodeObjects[i].data.numa = rn.numa;
                  if (Array.isArray(payload.matrix) && payload.matrix[i]) {
                    const gxx = payload.matrix[i][i] || 1.0;
                    const gyy = payload.matrix[i][(i + 1) % 20] || 0.95;
                    const gzz = payload.matrix[i][(i + 2) % 20] || 1.02;
                    this.nodeObjects[i].data.tensor = [gxx, gyy, gzz];
                    this.nodeObjects[i].tensorMesh.scale.set(gxx, gyy, gzz);
                  }
                }
              });
            } else if (Array.isArray(payload.nodes)) {
              // Suporte ao formato nodes direto
              payload.nodes.forEach((rn, i) => {
                if (this.nodeObjects[i] && rn.position) {
                  this.nodeObjects[i].basePos.set(rn.position[0], rn.position[1], rn.position[2]);
                  if (rn.tensor) {
                    this.nodeObjects[i].data.tensor = rn.tensor;
                    this.nodeObjects[i].tensorMesh.scale.set(rn.tensor[0], rn.tensor[1], rn.tensor[2]);
                  }
                  if (typeof rn.curvature === 'number') {
                    this.nodeObjects[i].data.curvature = rn.curvature;
                  }
                }
              });
            }

            const edgeList = payload.edges || payload.spectrum_1d;
            if (Array.isArray(edgeList) && edgeList.length > 0) {
              edgeList.forEach(re => {
                const local = this.edgeObjects.find(e => (e.from === re.from && e.to === re.to) || (e.from === re.to && e.to === re.from));
                if (local && typeof re.curvature === 'number') {
                  local.data.curvature = re.curvature;
                  const newCol = curvatureToColor(re.curvature);
                  local.material.color.copy(newCol);
                  local.material.emissive.copy(newCol);
                }
              });
            }

            if (Array.isArray(payload.triangles) && payload.triangles.length > 0) {
              this.triangles = payload.triangles;
              this.buildSimplexMesh();
            }
          }
        }
      } catch (e) {
        // Fallback silencioso: mantém geometria hiperbólica canônica sem quebras
        console.info('[KinePlex 3D] Operando com malha hiperbólica autônoma de alta fidelidade.');
      }
    }

    /**
     * Loop principal de renderização a 60 FPS
     */
    startRenderLoop() {
      const animate = (time) => {
        if (this.isDestroyed) return;
        requestAnimationFrame(animate);

        const sec = time * 0.001;

        // 1. Dinâmica da Câmera (Zoom interpolado suave)
        this.camera.position.z += (this.targetCameraZ - this.camera.position.z) * 0.12;

        // 2. Rotação com Inércia (Damping) ou Auto-rotação sutil
        if (this.isDragging) {
          // Movimento direto pelo mouse
        } else if (Math.abs(this.rotVelocity.x) > 0.0001 || Math.abs(this.rotVelocity.y) > 0.0001) {
          this.manifoldGroup.rotation.y += this.rotVelocity.y;
          this.manifoldGroup.rotation.x += this.rotVelocity.x;
          this.rotVelocity.x *= this.damping;
          this.rotVelocity.y *= this.damping;
        } else {
          // Órbita ambiente lenta
          this.manifoldGroup.rotation.y += 0.0016;
          this.manifoldGroup.rotation.x += 0.0005;
        }

        // 3. Física de Relaxamento Elástico dos Nós (Mola de Hooke amortecida)
        const kSpring = 0.085;
        const kDamping = 0.88;
        let positionsNeedUpdate = false;

        this.nodeObjects.forEach((nodeObj, idx) => {
          // Força restauradora elástica para a posição base
          const disp = new THREE.Vector3().subVectors(nodeObj.basePos, nodeObj.currentPos);
          if (disp.lengthSq() > 0.00001 || nodeObj.velocity.lengthSq() > 0.00001) {
            nodeObj.velocity.add(disp.multiplyScalar(kSpring));
            nodeObj.velocity.multiplyScalar(kDamping);
            nodeObj.currentPos.add(nodeObj.velocity);
            nodeObj.nodeContainer.position.copy(nodeObj.currentPos);
            positionsNeedUpdate = true;
          }

          // Pulsação contínua sutil do halo de cada nó
          const pulseScale = 1.0 + 0.12 * Math.sin(sec * 3.0 + idx * 0.7);
          nodeObj.haloMesh.scale.set(pulseScale, pulseScale, pulseScale);

          // Decaimento suave do flash do halo após morph
          if (nodeObj.haloMesh.material.opacity > 0.28 && this.hoveredNodeIndex !== idx) {
            nodeObj.haloMesh.material.opacity += (0.28 - nodeObj.haloMesh.material.opacity) * 0.06;
          }
        });

        // 4. Decaimento suave de emissão das arestas
        this.edgeObjects.forEach(edgeObj => {
          if (edgeObj.material.emissiveIntensity > 0.55) {
            edgeObj.material.emissiveIntensity += (0.55 - edgeObj.material.emissiveIntensity) * 0.05;
          }
        });

        // Atualiza buffers quando nós estiverem em movimento
        if (positionsNeedUpdate) {
          this.updateDeformedPositions();
        }

        // 5. Raycasting interativo no mouse
        this.checkRaycasting();

        // 6. Renderização do Frame
        this.renderer.render(this.scene, this.camera);
      };

      requestAnimationFrame(animate);
    }

    destroy() {
      this.isDestroyed = true;
      if (this.resizeObserver) this.resizeObserver.disconnect();
      if (this.renderer && this.renderer.domElement) {
        this.renderer.domElement.remove();
      }
    }
  }

  // ==============================================================
  // INTERFACES GLOBAIS DE INTEGRAÇÃO COM KINEPLEX UI              //
  // ==============================================================
  window.KinePlex3DManifold = KinePlex3DManifold;

  window.init3DManifold = function (containerId = 'threejs-manifold-container') {
    if (!window.kineplex3DInstance) {
      window.kineplex3DInstance = new KinePlex3DManifold(containerId);
    } else {
      window.kineplex3DInstance.resize();
    }
    return window.kineplex3DInstance;
  };

  window.reset3DManifold = function () {
    if (window.kineplex3DInstance) {
      window.kineplex3DInstance.resetCamera();
    }
  };

  window.morphManifoldShallow = function () {
    if (window.kineplex3DInstance) {
      window.kineplex3DInstance.morphManifoldShallow();
    }
  };

  window.changeVisMode = function (mode) {
    if (window.kineplex3DInstance) {
      window.kineplex3DInstance.changeVisMode(mode);
    }
  };

})();

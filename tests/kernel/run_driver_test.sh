#!/bin/bash
# SPDX-License-Identifier: GPL-2.0
# Teste automatizado de integração e segurança do Kernel Driver KinePlex
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
KEEP_LOADED=false

# Argumentos
NIC="${1:-}"
if [ "${NIC}" = "--keep" ] || [ "${NIC}" = "--keep-loaded" ]; then
    KEEP_LOADED=true
    NIC=""
fi
if [ "${2:-}" = "--keep" ] || [ "${2:-}" = "--keep-loaded" ]; then
    KEEP_LOADED=true
fi

echo "================================================================================"
echo "          KINEPLEX KERNEL DRIVER INTEGRATION & CAPABILITY TEST                 "
echo "================================================================================"

# 1. Detectar interface de rede caso não tenha sido informada
if [ -z "$NIC" ]; then
    # Prioriza interface com estado operacional UP (excluindo loopback)
    NIC=$(ip -br link show | awk '$1 != "lo" && $2 == "UP" {print $1}' | head -n1 || true)
    if [ -z "$NIC" ]; then
        # Fallback para primeira interface física disponível
        NIC=$(ip -br link show | awk '$1 != "lo" {print $1}' | head -n1 || true)
    fi
fi

if [ -z "$NIC" ]; then
    echo "[-] Erro: Nenhuma interface de rede detectada no sistema." >&2
    exit 1
fi
echo "[+] Interface de rede selecionada: ${NIC}"

# 2. Verificar/Compilar o módulo do kernel
MODULE_KO="$ROOT/kineplex-kernel/kineplex_geo.ko"
if [ ! -f "$MODULE_KO" ]; then
    echo "[*] Módulo não encontrado. Compilando kineplex_geo.ko..."
    make -C "$ROOT/kineplex-kernel"
fi
echo "[+] Binário do driver verificado: ${MODULE_KO}"

# 3. Preparar limpeza na saída (a menos que solicitado --keep)
cleanup() {
    local exit_code=$?
    if [ "$KEEP_LOADED" = false ]; then
        echo "[*] Descarregando módulo kineplex_geo..."
        if lsmod | grep -E -q '^kineplex_geo\b'; then
            sudo rmmod kineplex_geo || true
        fi
    else
        echo "[!] Módulo mantido carregado no kernel (--keep-loaded ativo)."
    fi
    exit $exit_code
}
trap cleanup EXIT INT TERM

# Descarregar se já estiver carregado antes de iniciar
if lsmod | grep -E -q '^kineplex_geo\b'; then
    echo "[*] Módulo já estava carregado no kernel. Descarregando versão anterior..."
    sudo rmmod kineplex_geo || true
fi

# 4. Carregar o módulo via sudo
echo "[*] Solicitando elevação para carregar o módulo no kernel..."
if ! sudo insmod "$MODULE_KO" nic_ifname="$NIC" 2>/dev/null; then
    sudo rmmod kineplex_geo 2>/dev/null || true
    sudo insmod "$MODULE_KO" nic_ifname="$NIC"
fi

# 5. Verificar criação do Character Device
if [ ! -c /dev/kinegeo ]; then
    echo "[-] Erro: Dispositivo de caracteres /dev/kinegeo não foi criado!" >&2
    sudo dmesg | grep -i "kineplex_geo" | tail -n 10 || true
    exit 1
fi

# Garante acesso ao device para que processos não-root alcancem a checagem CAP_NET_ADMIN
sudo chmod 0666 /dev/kinegeo 2>/dev/null || true

echo "[+] Dispositivo /dev/kinegeo criado e pronto:"
ls -l /dev/kinegeo

# 6. Exibir mensagens do dmesg
echo ""
echo "--- Logs recentes do kernel (dmesg) ---"
sudo dmesg | grep -i "kineplex_geo" | tail -n 5 || true
echo "---------------------------------------"
echo ""

# 7. Executar Capability Smoke Test (FT-096)
echo "[*] Executando teste de segurança (verificando restrição de CAP_NET_ADMIN)..."
if [ "$(id -u)" -eq 0 ] && [ -n "${SUDO_USER:-}" ]; then
    # Se o script inteiro foi chamado com sudo, baixa para o usuário real
    su - "$SUDO_USER" -c "$ROOT/tests/kernel/capability_smoke.sh /dev/kinegeo"
else
    "$ROOT/tests/kernel/capability_smoke.sh" /dev/kinegeo
fi

echo ""
echo "================================================================================"
echo "  [SUCESSO] Driver carregado, dispositivo validado e segurança de acesso OK!   "
echo "================================================================================"

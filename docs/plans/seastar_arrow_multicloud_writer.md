# Seastar C++ Engine: Arquitetura de Escrita Apache Arrow para Multi-Cloud Object Storage
## (AWS S3, Azure Blob, Google Cloud Storage, Alibaba Cloud OSS, Oracle Cloud Infrastructure)

Este documento especifica a arquitetura e a fundação do motor em **Seastar C++** para complemento e aceleração de escrita colunar Apache Arrow / Parquet no ecossistema KinePlex / KineHaus, direcionado aos 5 maiores provedores globais de nuvem:
1. **Amazon Web Services (AWS S3)**
2. **Microsoft Azure (Azure Blob Storage)**
3. **Google Cloud Platform (Google Cloud Storage - GCS)**
4. **Alibaba Cloud (Aliyun OSS)**
5. **Oracle Cloud Infrastructure (OCI Object Storage)**

---

## 1. Fundamentos do Seastar (https://seastar.io/)

O Seastar é um framework assíncrono em C++20 projetado para extrair 100% da capacidade do hardware moderno de alta concorrência:
- **Shared-Nothing (Thread-per-Core)**: Ao contrário de modelos multithread tradicionais (com pools de threads concorrendo por locks de mutex), o Seastar fixa uma thread por núcleo de CPU. Cada núcleo possui sua própria fila de eventos, alocador de memória local (`seastar::memory`) e reactor de I/O. Não há contenção de barramento de memória (NUMA-aware).
- **Kernel-Bypass e `io_uring`**: Operações de I/O de disco (para NVMe local de staging) e sockets de rede utilizam `io_uring` assíncrono nativo do Linux kernel 5.x+, eliminando trocas de contexto (*context switching*) e cópias entre espaço de kernel e espaço de usuário.
- **Futures, Promises e Coroutines (`co_await`)**: Fluxo de controle assíncrono sem bloqueios. Cada requisição HTTP/REST para a nuvem é tratada como um fluxo contínuo de buffers não-bloqueantes.

---

## 2. O Desafio de Impedância: Arrow, Bloqueios de Nuvem e a Solução Seastar

### O Problema com SDKs Convencionais
Os SDKs oficiais em C++ (AWS C++ SDK, Azure Storage SDK, Google Cloud C++ Client, Aliyun SDK, OCI SDK) dependem de:
1. Sockets síncronos bloqueantes ou pools genéricos de `std::thread` / `pthread`.
2. Alocações globais em heap (`malloc`/`free`) que competem por arenas e degradam a localidade de cache L1/L2/L3.
3. Se um shard Seastar invocar qualquer método bloqueante de rede, todo o núcleo da CPU congela, destruindo a vazão de centenas de gigabits por segundo (100GbE / 400GbE).

### A Solução KinePlex: Seastar Multi-Cloud HTTP/2 REST Reactor
O motor substitui bibliotecas bloqueantes por um cliente HTTP/1.1 e HTTP/2 assíncrono nativo construído sobre o **Seastar Network Stack**, implementando o protocolo REST e a autenticação criptográfica pura (SHA-256 / HMAC / RSA) de cada nuvem diretamente nos buffers Arrow.

```
+-------------------------------------------------------------------------+
|                       KinePlex / KineHaus Core                         |
|     (Laguerre-Kantorovich Optimal Distributor & Arrow RecordBatches)    |
+-------------------------------------------------------------------------+
                                    | (Zero-Copy C ABI Pointer Hand-off)
                                    v
+-------------------------------------------------------------------------+
|                   kineplex-seastar-io (C++ Bridge)                      |
|                                                                         |
|  [ Shard 0 (Core 0) ]       [ Shard 1 (Core 1) ]      [ Shard N (Core N)]
|  +-----------------+       +------------------+      +-----------------+
|  | Seastar Reactor |       | Seastar Reactor  |      | Seastar Reactor |
|  |   - io_uring    |       |   - io_uring     |      |   - io_uring    |
|  |   - Memory Pool |       |   - Memory Pool  |      |   - Memory Pool |
|  +--------+--------+       +--------+---------+      +--------+--------+
|           |                         |                         |
|           +-------------------------+-------------------------+
|                                     |
|           [ Asynchronous Multi-Cloud REST Engine (HTTP/1.1 & HTTP/2) ]  |
+-------------------------------------+-----------------------------------+
                                      |
       +------------------------------+------------------------------+
       |               |              |              |               |
       v               v              v              v               v
  +----------+   +-----------+  +----------+   +----------+   +--------------+
  |  AWS S3  |   | Azure     |  | Google   |   | Alibaba  |   | Oracle OCI   |
  |  (SigV4) |   | Blob      |  | GCS      |   | OSS      |   | ObjectStore  |
  +----------+   +-----------+  +----------+   +----------+   +--------------+
```

---

## 3. Especificação dos 5 Adaptadores de Nuvem em Seastar C++

Cada nuvem é suportada através de uma interface assíncrona unificada:
```cpp
class cloud_object_sink {
public:
    virtual seastar::future<upload_session> initiate_upload(std::string_view path) = 0;
    virtual seastar::future<part_upload_result> upload_part(upload_session& session, 
                                                            uint32_t part_number, 
                                                            seastar::net::packet data) = 0;
    virtual seastar::future<file_metadata> complete_upload(upload_session& session, 
                                                           std::vector<part_upload_result> parts) = 0;
    virtual seastar::future<> abort_upload(upload_session& session) = 0;
    virtual ~cloud_object_sink() = default;
};
```

### 3.1. AWS S3 (Amazon Web Services)
- **Autenticação**: AWS Signature Version 4 (HMAC-SHA256 calculado via SSE/AVX-512 sobre o cabeçalho canônico).
- **Mecanismo de Escrita**:
  - `POST /{Key}?uploads` -> Retorna `UploadId`.
  - `PUT /{Key}?partNumber={N}&uploadId={Id}` -> Envia buffers de 16MB a 128MB com `x-amz-content-sha256: UNSIGNED-PAYLOAD` em TLS para máxima taxa de transferência.
  - `POST /{Key}?uploadId={Id}` com corpo XML contendo `<PartNumber>` e `<ETag>`.

### 3.2. Microsoft Azure Blob Storage
- **Autenticação**: Shared Key via `Authorization: SharedKey {account}:{signature}` ou Azure Entra ID Bearer Token atualizado assincronamente.
- **Mecanismo de Escrita (Block Blob)**:
  - `PUT /{container}/{blob}?comp=block&blockid={base64_id}` -> Envia cada bloco de dados Arrow/Parquet sem necessidade de handshake prévio.
  - `PUT /{container}/{blob}?comp=blocklist` -> Comita a lista ordenada de blocos com propriedades atômicas e metadados de catálogo.

### 3.3. Google Cloud Storage (GCS)
- **Autenticação**: OAuth2 Bearer Tokens gerenciados por `seastar::timer` ou chaves HMAC compatíveis com S3 XML API.
- **Mecanismo de Escrita**:
  - Protocolo **Resumable Uploads**:
    - `POST /upload/storage/v1/b/{bucket}/o?uploadType=resumable` -> Retorna URI de upload de sessão.
    - `PUT {Session-URI}` com cabeçalhos `Content-Range: bytes {start}-{end}/{total}` para streaming contínuo de blocos de 16MB.

### 3.4. Alibaba Cloud OSS (Aliyun)
- **Autenticação**: OSS Signature V4 (`x-oss-signature-version: OSS4-HMAC-SHA256`).
- **Mecanismo de Escrita**:
  - `POST /{Key}?uploads` -> Retorna `UploadId`.
  - `PUT /{Key}?partNumber={N}&uploadId={Id}` -> Upload concorrente de partes balanceadas por shard Seastar.
  - `POST /{Key}?uploadId={Id}` com XML de partes para finalização atômica.

### 3.5. Oracle Cloud Infrastructure (OCI Object Storage)
- **Autenticação**: OCI Request Signature (cabeçalhos HTTP assinados com chave privada RSA SHA-256: `(request-target)`, `host`, `date`, `x-content-sha256`).
- **Mecanismo de Escrita**:
  - `POST /n/{namespace}/b/{bucket}/u` -> `CreateMultipartUpload`.
  - `PUT /n/{namespace}/b/{bucket}/u/{objectName}?uploadId={Id}&uploadPartNum={N}` -> Upload de partes.
  - `POST /n/{namespace}/b/{bucket}/u/{objectName}?uploadId={Id}` -> `CommitMultipartUpload`.

---

## 4. Integração C ABI / Rust FFI: Ponte KinePlex <-> Seastar

Para integrar o `LaguerreOptimalDistributor` em Rust com o reactor Seastar em C++ com zero cópia de memória:

```c
// Interface C ABI exportada pelo runtime Seastar
typedef struct {
    void* seastar_runtime_handle;
} seastar_cloud_engine_t;

typedef struct {
    const uint8_t* data;
    size_t length;
    uint32_t part_index;
} arrow_memory_chunk_t;

// Funções da Ponte FFI
seastar_cloud_engine_t* kineplex_seastar_init(uint32_t thread_count);
void kineplex_seastar_destroy(seastar_cloud_engine_t* engine);

int32_t kineplex_seastar_submit_part(
    seastar_cloud_engine_t* engine,
    const char* cloud_provider, // "aws", "azure", "google", "alibaba", "oracle"
    const char* bucket_name,
    const char* object_key,
    arrow_memory_chunk_t chunk,
    void (*completion_cb)(void* user_ctx, int32_t status_code, const char* etag),
    void* user_ctx
);
```

No lado Rust (`kineplex-core`), a camada `LaguerreParquetSink` implementa este despacho:
- Em ambiente de desenvolvimento e testes locais, utiliza o `InMemoryParquetSink` ou `FileParquetSink` nativos.
- Em ambiente de produção bare-metal/nuvem, ativa o driver Seastar via FFI, permitindo taxas de saturação de link de até 100Gbps por nó sem overhead de kernel.

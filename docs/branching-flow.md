# Fluxo de branches do KinePlex

## Cadeia obrigatória

```text
feat/XXX-name -> develop -> homolog -> main
```

- `feat/XXX-name`: desenvolvimento de uma tarefa isolada. Exemplo: `feat/FT-071-ci-branch-flow`.
- `develop`: integração contínua das features aprovadas.
- `homolog`: candidato de homologação e validação de estabilidade.
- `main`: produção. O branch `main` deve representar somente releases aprovados.

As promoções devem ocorrer exclusivamente por Pull Request, nesta ordem:

1. `feat/*` para `develop`
2. `develop` para `homolog`
3. `homolog` para `main`

Não são permitidos Pull Requests que pulem um ambiente. O workflow `KinePlex CI` verifica automaticamente a direção da origem e do destino.

## Gates de teste

Todos os Pull Requests da cadeia executam:

- `cargo test --workspace --locked`;
- `geometry_stress`;
- `geometry_allocations`;
- `geometry_deformation_stress`;
- `plane_isolation_io_contamination`;
- validação sintática do harness Python;
- verificação estrutural dos hooks do driver kernel.

O benchmark de geometria permanece no workflow semanal/manual `Geometry stability`. O E2E Docker não é gate obrigatório porque o data plane ainda não produz o Parquet final, conforme documentado em `docs/geometry-stability-v0.2.md`.

## Proteção no GitHub

Configure proteção de branch para `develop`, `homolog` e `main` exigindo Pull Request e os checks abaixo antes do merge:

- `Branch flow policy`;
- `Rust build and tests`;
- `E2E harness syntax`;
- `Kernel driver source checks`.

Também devem ser bloqueados push direto, force-push e merge sem branch atualizado. A proteção de `main` deve exigir revisão de release. A API usada nesta sessão não permitiu consultar ou alterar as regras de proteção; portanto, essa configuração deve ser confirmada por um administrador no GitHub.

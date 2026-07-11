---
name: itx-document-script
description: Use when asked to document, standardize, or expand docstrings/comments on a .py file under glue/scripts/ or lambdas/ in the interchange-datalake-aws project — e.g. "documenta este archivo", "aplica el estándar de documentación", "agrega docstrings a este script de glue/lambda", "estandariza la documentación de <archivo>", "usa la skill itx-document-script", "qué archivos faltan documentar".
version: 2.0.0
---

# Documentación estándar de scripts Glue/Lambda (itx-document-script)

Aplica al código de este repo (`interchange-datalake-aws`) el mismo patrón de
documentación ya usado en ~26 archivos de `glue/scripts/` y `lambdas/`
(sesión 2026-07-10). Objetivo de esta skill: no volver a re-derivar el
formato leyendo 4-5 archivos de ejemplo cada vez — el formato ya está fijado
acá, con ejemplos reales en `references/`.

## Regla de oro — no negociable

**Se edita SOLO texto dentro de docstrings (y, si aplica, comentarios `#`
genuinamente aclaratorios). Cero cambios de lógica, nombres, tipos, imports,
orden de imports, formato/estilo o blank lines fuera de docstrings.**

Motivo: estos son los scripts reales que corren en producción (Glue jobs y
Lambdas ya desplegados y validados contra legacy). Un cambio de lógica
disfrazado de "documentación" es el peor tipo de regresión — invisible en un
review rápido del diff.

Antes de dar por cerrado un archivo, correr el verificador (abajo) y
confirmar que imprime `DOC-ONLY`. Si imprime `LOGIC CHANGED`, hay un cambio
accidental de código que hay que revisar y revertir antes de seguir.

No correr formatters (Ruff, black, isort, etc.) sobre el archivo — el
usuario ya tiene su propio formateador de Python vía extensión y no quiere
reformateo manual mezclado con esto (ver memoria `feedback_python_formatter`).

**Funciones triviales de una línea** (ej. `def log_info(msg): logger.info(...)`)
se dejan sin docstring — agregarles uno obliga a partirlas en multilínea, lo
cual ya es tocar formato/estructura del código, no solo texto. Solo aplica a
funciones genuinamente autoexplicativas por su nombre + cuerpo.

## Limpieza de comentarios `#` redundantes

Al expandir el docstring de una función, es común que termine explicando lo
mismo que ya decía un comentario `#` suelto dentro del cuerpo (mismo
razonamiento, dicho dos veces en dos lugares distintos). Cuando eso pase,
**borrar el comentario `#` que quedó duplicado** — quedarse con una sola
fuente de verdad (el docstring).

Esto SÍ está permitido por la regla de oro: los comentarios `#` no forman
parte del AST de Python, así que borrarlos no afecta al verificador
(`verify_docs_only.py` sigue reportando `DOC-ONLY`).

Cómo decidir si un comentario es candidato a borrar:
- **Borrar** si el comentario es un párrafo de razonamiento que el docstring
  ya reformuló casi palabra por palabra (ej. "por qué se preserva el índice
  antes del merge", "por qué CAL tiene prioridad sobre CLN al hacer join").
- **NO borrar** si es solo una etiqueta corta de sección dentro de una
  función larga (ej. `# OPT 1: ...`, `# 1. Renombrar columnas...`,
  `# Blindaje de tipos`) — ayudan a navegar el código a simple vista y no
  duplican prosa.
- **NO borrar** si tiene un dato que el docstring no repite (un número, un
  caso límite puntual) — ahí lo mejor es que el docstring **apunte** al
  comentario en vez de duplicarlo (ver ejemplo de `calculate_fee_amounts` en
  `references/example_glue.md` — el docstring dice "ver el comentario
  inline antes del join" en vez de repetir la fórmula completa). Preferir
  apuntar sobre duplicar evita tener que hacer esta limpieza después.

## Dos variantes de encabezado de módulo

### 1. Entry-point (`handler.py` de una Lambda, o script principal de un Glue job)

**Usar literalmente "Lambda real:" o "Job real:"** (no sinónimos como "Glue
ETL Job:", "AWS Lambda:", etc. — un canario con `haiku` derivó a esa
paráfrasis y rompió la consistencia con el resto de los archivos ya hechos).
Igual de literal: **"Flujo:"** para la sección de pasos numerados (no
"Lógica interna:" ni variantes). El párrafo de prosa se ajusta a ~80
columnas por línea (wrap manual), igual que el resto del repo — un párrafo
de 400 caracteres en una sola línea es una señal de que no se aplicó el
wrap.

```
"""
<archivo>.py — <Lambda real|Job real>: <nombre-real-del-recurso-en-AWS>
================================================================================
Archivo:     <path/relativo/desde/la/raiz/del/repo.py>
S3 Script:   s3://<bucket-reference>/<ruta-real-en-s3>          ← SOLO Glue jobs

<Uno o más párrafos de prosa: qué hace el archivo, su lugar en el pipeline,
qué lee/escribe, decisiones no obvias si las hay.>

[Secciones opcionales — usar solo las que apliquen, no todas a la vez:]
  Flujo: <pasos numerados, si hay un pipeline interno secuencial claro>
  Variables de entorno: <NOMBRE : descripción>        ← típico en Lambdas
  Job Parameters: <--arg   descripción>                 ← típico en Glue jobs
  Estructura S3 esperada: <árbol de paths de entrada/salida>
  Database / Input / Output: <para jobs simples de 1 tabla>
  Fuentes de datos / Salida: <en jobs de reporting>
  SIMPLIFICACIONES CONOCIDAS: <limitaciones ya documentadas, no bloqueantes>
"""
```

Ejemplos reales completos → `references/example_lambda.md` (Lambda) y
`references/example_glue.md` (Glue job).

### 2. Módulo interno (importado por un entry-point, no desplegado directamente)

Ej: `ardef/calculate.py`, `iar/extract.py`, `persistence/database.py`.

```
"""
<nombre_archivo.py>

<Uno o más párrafos de prosa: rol de este módulo dentro de la etapa del
pipeline que lo importa.>
"""
```

Sin título largo, sin "Lambda real:"/"Job real:", sin "Archivo:"/
"S3 Script:" — solo el nombre de archivo. El resto del archivo (funciones,
clases) recibe el mismo nivel de detalle que en un entry-point.

## Plantilla de docstring de función/método

```python
def nombre_funcion(param1: tipo, param2: tipo = default) -> tipo_retorno:
    """
    <Una o más oraciones: QUÉ hace la función y, si no es obvio, POR QUÉ
    existe o qué decisión de diseño encarna. No repetir el nombre de la
    función en la primera línea.>

    [Conservar/expandir cualquier bloque de razonamiento que ya existía:
    "Por qué X:", "Estrategia:", fórmulas, listas con "-". Nunca borrar
    contexto de diseño ya documentado — solo completar lo que falta.]

    Args:
        param1: Descripción breve; agregar ejemplo inline si el valor no es
            obvio, ej. "VISA260416.zip".
        param2: Descripción breve.

    Returns:
        Qué devuelve y en qué formato (o el efecto/excepción si no retorna
        nada útil — ver ejemplo de función sin retorno en
        references/example_lambda.md).

    Ejemplo:
        nombre_funcion("valor_ejemplo", 123)  # -> resultado o comportamiento esperado
    """
```

Reglas de la plantilla:
- **Sin parámetros** → omitir el bloque `Args:` completo (no escribir
  "Args: Ninguno").
- **`Returns:` siempre presente**, incluso si la función no retorna nada útil
  (describir el side-effect o la excepción que puede lanzar).
- **`Ejemplo:`** casi siempre presente — una línea de invocación realista con
  el resultado/comportamiento esperado en comentario.
- Aplicar a **todas** las funciones y métodos del archivo, incluyendo
  helpers `_privados` y `__init__` de clases.
- Las clases también llevan docstring expandido (qué encapsulan, no repetir
  el nombre de la clase).

## Profundidad escalonada (Tier 1 vs Tier 2)

No todo archivo justifica el mismo nivel de detalle. Clasificar ANTES de
escribir nada:

**Tier 1 — tratamiento completo** (Args + Returns + Ejemplo en cada función):
entry-points (`handler.py`, scripts principales de Glue) y módulos de lógica
de negocio real (cálculo de fees, parsing de formatos IPM/ISO-8583,
transformación de datos, reglas de interchange). Son los que alguien va a
necesitar entender a fondo para debuggear o modificar.

**Tier 2 — tratamiento liviano** (docstring de 1-2 oraciones, `Args`/
`Returns` solo si agregan algo no obvio, se omite `Ejemplo` salvo que
aclare algo genuinamente confuso): módulos de soporte/utilitarios de bajo
riesgo — loggers, wrappers delgados de persistencia (S3/DynamoDB sin lógica
propia), definiciones de schema/constantes, clases de configuración simples.
El criterio: si la función es autoexplicativa por su nombre + 3 líneas de
cuerpo, un docstring largo es ruido, no documentación.

Ante la duda, Tier 1. El encabezado de módulo (las 2 variantes de arriba)
NO cambia por tier — la escalada de profundidad aplica solo a nivel función/
método.

## Aplicar docstrings sin gastar un `Edit` por función (recomendado para lotes)

Para archivos con muchas funciones (o cuando se procesan varios archivos en
un subagente), NO hacer un `Edit` por función — cada uno es un tool call
completo que reenvía el historial acumulado y multiplica el costo (medido:
~150-180 tokens/línea de código con el enfoque de un-Edit-por-función en un
lote de 21 archivos). En su lugar:

1. `Read` el archivo completo (una vez).
2. Correr `python .claude/skills/itx-document-script/scripts/apply_docstrings.py --file <archivo.py> --list`
   para ver los qualnames disponibles (nombres de función/clase/método,
   anidamiento con `.`, más `MODULE` para el docstring de módulo).
3. Redactar TODOS los docstrings de una sola vez, como un dict qualname →
   texto (sin comillas triples, sin indentar — el script se encarga), y
   escribirlo con `Write` a un JSON temporal (ej. en el scratchpad de la
   sesión, no en el repo).
4. Aplicar todo de una vez:
   ```
   python .claude/skills/itx-document-script/scripts/apply_docstrings.py --file <archivo.py> --manifest <manifiesto.json>
   ```
   El script inserta o reemplaza cada docstring en su posición exacta
   (detecta indentación y si ya existe un docstring para reemplazar vs.
   insertar) sin tocar ninguna otra línea.
5. Verificar igual que siempre: `verify_docs_only.py <archivo.py>` → debe
   decir `DOC-ONLY`.

Esto baja un archivo de ~10-15 tool calls (un `Edit` por función) a ~4
(Read, list, write manifiesto, apply + verify) sin sacrificar profundidad —
el modelo sigue redactando el 100% de la prosa, solo cambia cómo se aplica.

Si el archivo es chico (pocas funciones) o se está documentando a mano en
la conversación principal (no via subagente), seguir usando `Edit` función
por función sigue siendo válido — el manifiesto rinde más cuando hay
volumen.

## Uso vía subagentes en background (lotes grandes)

Cuando se delega esta skill a un subagente (Task/Agent tool) para procesar
muchos archivos:

- **Prompt de lanzamiento corto**: decir "leé `.claude/skills/itx-document-script/SKILL.md`
  y aplicalo a estos archivos: [lista]" + el contexto mínimo de negocio que
  el agente no puede inferir solo leyendo el código (para qué sirve el
  módulo, qué Lambda/Glue job real es). NO reexplicar las plantillas ni las
  reglas del SKILL.md dentro del prompt — el agente las lee solo, y
  duplicarlas en el prompt es contexto pagado dos veces.
- **Lotes grandes, no chicos**: agrupar tantos archivos afines como sea
  razonable en un solo subagente (ej. un módulo completo) en vez de muchos
  lotes de 1-2 archivos — el costo fijo de leer `SKILL.md`+`references/` se
  paga una vez por subagente, no una vez por archivo.
- **Modelo**: para lotes de módulos utilitarios/Tier 2 (mecánicos, bajo
  riesgo de interpretación), se puede usar `model: "haiku"` en el Agent
  call. Antes de comprometer un lote grande a haiku, correr un canario de
  1 archivo chico y revisar la calidad (¿preservó el razonamiento "por qué"
  ya existente? ¿decidió bien qué comentarios `#` borrar?) antes de
  escalar. Para Tier 1 (lógica de negocio, entry-points complejos) usar el
  modelo por defecto — ahí sí importa el criterio fino.
  **Hallazgo de un canario real (`format_exchange_rates.py`, 2026-07-11):**
  el contenido semántico de haiku fue correcto, pero derivó en 3 puntos de
  la convención EXACTA sin más refuerzo: tituló "Glue ETL Job:" en vez de
  "Job real:", usó "Lógica interna:" en vez de "Flujo:", y no aplicó el
  wrap de ~80 columnas (un párrafo quedó en una sola línea de ~400
  caracteres). Ninguno rompe `DOC-ONLY`, pero sí la consistencia visual
  entre archivos. Mitigación: el prompt de lanzamiento para haiku debe
  citar EXPLÍCITAMENTE los literales obligatorios ("Job real:"/"Lambda
  real:", "Flujo:") y pedir wrap a 80 columnas — no asumir que con solo
  leer `SKILL.md` los va a reproducir letra por letra. Con sonnet (el
  modelo usado en los lotes 1-4 de esta skill) no se observó este problema.
- **Medir antes de comprometerse**: lanzar 1 lote de prueba, mirar
  `<subagent_tokens>` en la notificación de finalización, proyectar el
  costo total, y recién ahí decidir cuántos lotes más lanzar en la misma
  sesión.

## Flujo de trabajo por archivo

0. **Chequeo de idempotencia (antes de leer nada más).**
   ```
   python .claude/skills/itx-document-script/scripts/find_undocumented.py <archivo.py>
   ```
   Si dice `[x] YA DOCUMENTADO`, **PARAR y preguntar al usuario** qué quiere
   en vez de asumir:
   - No hacer nada (ya está bien, se pidió por error).
   - Auditoría liviana: repasar función por función solo para detectar
     huecos puntuales (falta `Args:`/`Returns:`/`Ejemplo:` en alguna, quedó
     un comentario redundante) sin reescribir lo que ya está completo.
   - Re-pase completo a propósito (ej. porque el código real cambió después
     de documentarlo y los docstrings quedaron desactualizados).
   No asumir ninguna de las tres por default — correr la skill dos veces
   sobre el mismo archivo sin este chequeo es exactamente el desperdicio de
   tokens que la skill existe para evitar.
1. **Elegir el archivo** — si no hay uno indicado, correr el discovery
   script (abajo) para ver qué falta.
2. `Read` el archivo completo una sola vez.
3. Clasificar: ¿es entry-point o módulo interno? (ver arriba). Escribir/
   expandir el docstring de módulo con `Edit`.
4. Recorrer **todas** las funciones/clases del archivo, clasificar cada una
   Tier 1 vs Tier 2 (ver arriba), y expandir su docstring — con `Edit`
   función por función (archivos chicos / trabajo manual en la conversación
   principal) o con el flujo de manifiesto + `apply_docstrings.py` (lotes
   grandes / subagentes, ver sección correspondiente arriba). Funciones
   triviales de una línea quedan sin docstring (ver nota en "Regla de oro").
   Revisar si algún comentario `#` cercano quedó redundante con lo que se
   acaba de escribir (ver sección "Limpieza de comentarios `#` redundantes")
   y borrarlo si aplica.
5. **Verificar antes de dar el archivo por terminado:**
   ```
   python .claude/skills/itx-document-script/scripts/verify_docs_only.py <archivo.py>
   ```
   Debe imprimir `DOC-ONLY`. Si imprime `LOGIC CHANGED`, revisar el diff
   (`git diff -- <archivo.py>`) y corregir antes de continuar.
6. **No commitear** — el usuario hace sus propios commits (ver memoria
   `feedback_git_commits`).

## Descubrir qué archivos faltan

```
python .claude/skills/itx-document-script/scripts/find_undocumented.py
```

Lista todo `.py` bajo `glue/scripts/` y `lambdas/` como `[x]` (ya tiene uno
de los dos encabezados válidos) o `[ ]` (pendiente). Es un heurístico basado
en el encabezado, no infalible — para archivos triviales (`__init__.py`
vacíos, wrappers de 5 líneas) usar criterio: si no hay nada sustancial que
explicar, no forzar la plantilla completa.

## Ahorro de tokens — por qué esta skill existe

Una sesión anterior hizo este mismo trabajo archivo por archivo re-derivando
el formato cada vez (leyendo varios archivos ya hechos para "recordar" el
estilo) — costoso en tokens. Con esta skill:
- El formato exacto vive acá, no hay que re-leerlo de otros archivos.
- El verificador AST (`verify_docs_only.py`) reemplaza una revisión manual
  línea por línea del diff para confirmar "solo documentación".
- El discovery script reemplaza tener que preguntar o adivinar qué archivos
  ya se hicieron.
- `apply_docstrings.py` reemplaza el patrón "un `Edit` por función" (costoso
  en tool calls a escala) por un manifiesto redactado de una sola vez +
  una aplicación determinística vía AST.

Referencias completas con ejemplos reales (antes/después) →
`references/example_lambda.md`, `references/example_glue.md`.

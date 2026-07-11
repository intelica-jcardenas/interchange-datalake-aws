# Ejemplo real — Lambda entry-point (`lambdas/router/src/handler.py`)

## Encabezado de módulo — ANTES

```python
"""
Lambda Router - itl-0004-itx-dev-intchg-02-lmbd-router
===========================
Trigger: S3 Event Notification cuando llega un archivo a Landing.
...
"""
```

## Encabezado de módulo — DESPUÉS

```python
"""
handler.py — Lambda real: itl-0004-itx-dev-intchg-02-lmbd-router
================================================================================
Archivo:     lambdas/router/src/handler.py

Punto de entrada del pipeline de interchange. Se dispara por un evento S3
ObjectCreated cuando llega un archivo nuevo al bucket de landing. Extrae el
client_id del path, clasifica el archivo contra los patrones regex
configurados en DynamoDB (file_pattern), calcula su MD5 en streaming para
detectar duplicados o nuevas versiones del mismo archivo, extrae la fecha de
negocio del contenido (con lógica distinta según sea Visa, Mastercard, IAR o
ARDEF) y registra el archivo en DynamoDB (file_control). Según la
clasificación, despacha el archivo a la siguiente etapa: la Step Function de
Visa o de Mastercard para el flujo transaccional normal (IN/OUT), directamente
a las Lambdas de reglas (vi-ardef / mc-iar) sin pasar por Step Functions, o a
la Lambda de descompresión (unzip) si el archivo es un ZIP.

Flujo:
1. Parsear evento S3 → bucket/key
2. Extraer client_id del path
3. Detectar si es ZIP → delegar a itx-unzip asincrónicamente
4. Cargar patrones de DynamoDB
5. Clasificar archivo con regex
6. Extraer fecha del header (solo 50 bytes, sin descargar todo)
7. Calcular MD5 en streaming (sin cargar todo el archivo en memoria)
8. Verificar duplicado en DynamoDB
9. Registrar en DynamoDB
10. Iniciar Step Functions

Variables de entorno:
  S3_BUCKET_LANDING            : bucket de landing
  DYNAMODB_TABLE_FILE_CONTROL  : tabla de control (default: itx-file-control)
  DYNAMODB_TABLE_FILE_PATTERN  : tabla de patrones (default: itx-file-pattern)
  STEP_FUNCTION_VI_ARN         : ARN de la Step Function Visa
  STEP_FUNCTION_MASTERCARD_ARN : ARN de la Step Function Mastercard
  VISA_ARDEF_FUNCTION_NAME     : ARN de la Lambda Visa ARDEF
  MASTERCARD_IAR_FUNCTION_NAME : ARN de la Lambda Mastercard IAR
  UNZIP_FUNCTION_NAME          : nombre de la Lambda unzip (default: itx-unzip)
"""
```

Notar: la sección "Variables de entorno" es específica de Lambdas (no aparece
en Glue jobs, que en cambio usan "Job Parameters" — ver `example_glue.md`).
"Flujo" con pasos numerados se usa quando el archivo tiene un pipeline interno
secuencial claro; es opcional, no forzar si no aporta.

## Docstring de función — ANTES

```python
def _extraer_fecha_de_zip(filename: str) -> str:
    """
    Extrae la fecha del nombre del archivo ZIP.
    Soporta dos formatos presentes en los clientes:
      YYYYMMDD: 20260416visaout.zip → 2026-04-16
                20260416mcin.zip    → 2026-04-16
      YYMMDD:   MAST260416.zip      → 2026-04-16
                VISA260416.zip      → 2026-04-16

    Estrategia:
      1. Buscar YYYYMMDD primero (8 dígitos) — más específico
      2. Si no → buscar YYMMDD (6 dígitos)
      3. Si no → fecha actual como fallback
    """
```

## Docstring de función — DESPUÉS

```python
def _extraer_fecha_de_zip(filename: str) -> str:
    """
    Extrae la fecha de negocio a partir del nombre del archivo ZIP, antes de
    descomprimirlo, para poder invocar la Lambda de unzip con esa fecha.

    Soporta dos formatos presentes en los clientes:
      YYYYMMDD: 20260416visaout.zip → 2026-04-16
                20260416mcin.zip    → 2026-04-16
      YYMMDD:   MAST260416.zip      → 2026-04-16
                VISA260416.zip      → 2026-04-16

    Estrategia:
      1. Buscar YYYYMMDD primero (8 dígitos) — más específico
      2. Si no → buscar YYMMDD (6 dígitos)
      3. Si no → fecha actual como fallback

    Args:
        filename: Nombre del archivo ZIP, ej. "VISA260416.zip".

    Returns:
        Fecha en formato "YYYY-MM-DD", o la fecha actual si no se pudo extraer.

    Ejemplo:
        _extraer_fecha_de_zip("20260416visaout.zip")  # "2026-04-16"
    """
```

Qué cambió exactamente: la primera oración se reescribió para explicar el
**por qué** (antes de descomprimir, para invocar unzip con esa fecha) en vez
de solo el qué. Todo el bloque "Estrategia:" (razonamiento ya existente) se
conservó intacto — nunca se borra contexto de diseño ya documentado, solo se
completa lo que falta (`Args`/`Returns`/`Ejemplo`).

## Función sin parámetros (caso simple)

```python
def validar_configuracion():
    """
    Verifica que todas las variables de entorno requeridas por el handler
    estén configuradas antes de procesar cualquier evento.

    Returns:
        None. Lanza ValueError si falta alguna variable requerida.

    Ejemplo:
        validar_configuracion()  # raise ValueError si falta STEP_FUNCTION_VI_ARN
    """
```

Sin parámetros → se omite el bloque `Args:` por completo (no escribir
`Args: Ninguno` ni nada equivalente). `Returns:` se mantiene igual —
describe el efecto/side-effect (acá, la excepción que puede lanzar) aunque
la función no retorne un valor útil.

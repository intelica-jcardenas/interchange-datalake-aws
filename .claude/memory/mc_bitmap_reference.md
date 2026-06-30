# Referencia: Parsing de Bitmap Mastercard IPM

Creado: 2026-06-24. Contexto: análisis de bitmaps de mensajes IPM 1240 para casuística con cliente.

---

## Qué es el bitmap

En el formato ISO-8583 (IPM Mastercard), cada mensaje tiene la estructura:

```
[4 bytes RDW] [4 bytes MTI] [8 o 16 bytes bitmap] [body con DEs]
```

El bitmap es una secuencia de bits donde cada bit indica si el Data Element (DE) correspondiente está presente en el mensaje:
- **Bit 1 (MSB del byte 0) = 1** → bitmap extendido (16 bytes, DEs 1–128)
- **Bit 1 (MSB del byte 0) = 0** → bitmap simple (8 bytes, DEs 1–64)

Convención: el bit más significativo (MSB) de cada byte corresponde al DE de menor número del grupo.

---

## Cómo parsear (Python)

Replica exactamente la lógica de `bitmap_bits()` en `lambdas/mastercard/interpreter/src/handler.py:757`.

```python
def parse_bitmap(data: bytes | str) -> list[int]:
    """Recibe bitmap como bytes o hex string (8 o 16 bytes). Devuelve DEs presentes."""
    if isinstance(data, str):
        data = bytes.fromhex(data.replace(" ", ""))
    fields = []
    for i, byte in enumerate(data):
        for bit in range(8):
            if byte & (1 << (7 - bit)):
                fields.append(i * 8 + bit + 1)
    return fields

# Ejemplo:
parse_bitmap("FCD00742856 1E20202000 00C108000 00")
# → [1, 2, 3, 4, 5, 6, 9, 10, 12, 22, 23, 24, 26, 31, 33, 38, 40, 42, 43, 48, 49, 50, 51, 55, 63, 71, 93, 94, 100, 105]
```

---

## Bitmaps analizados (2026-06-24)

### Bitmap A — `FC D0 07 42 84 61 E2 02 02 00 00 0C 10 80 00 00`

```
Byte  Hex  Binario    DEs
──────────────────────────────────────
  0   FC   11111100   1, 2, 3, 4, 5, 6
  1   D0   11010000   9, 10, 12
  2   07   00000111   22, 23, 24
  3   42   01000010   26, 31
  4   84   10000100   33, 38
  5   61   01100001   42, 43, 48
  6   E2   11100010   49, 50, 51, 55
  7   02   00000010   63
── bitmap secundario (DEs 65–128) ──
  8   02   00000010   71
  9   00   —
 10   00   —
 11   0C   00001100   93, 94
 12   10   00010000   100
 13   80   10000000   105
 14   00   —
 15   00   —
```

**DEs presentes (29):** 1, 2, 3, 4, 5, 6, 9, 10, 12, 22, 23, 24, 26, 31, 33, 38, 42, 43, 48, 49, 50, 51, 55, 63, 71, 93, 94, 100, 105

---

### Bitmap B — `FC D0 07 42 85 61 E2 02 02 00 00 0C 10 80 00 00`

Idéntico al Bitmap A excepto **byte 4: `84` → `85`** (`10000100` → `10000101`).

**Único cambio:** se agrega **DE 40** (bit LSB del byte 4).

**DEs presentes (30):** 1, 2, 3, 4, 5, 6, 9, 10, 12, 22, 23, 24, 26, 31, 33, 38, **40**, 42, 43, 48, 49, 50, 51, 55, 63, 71, 93, 94, 100, 105

---

## Tabla de DEs presentes en estos mensajes

| DE | Descripción Mastercard | Longitud / Tipo |
|----|------------------------|-----------------|
| 1  | Secondary Bitmap | 8 bytes fijo |
| 2  | Primary Account Number (PAN) | var, LLVAR |
| 3  | Processing Code | 6 num fijo |
| 4  | Amount, Transaction | 12 num fijo |
| 5  | Amount, Settlement | 12 num fijo |
| 6  | Amount, Cardholder Billing | 12 num fijo |
| 9  | Conversion Rate, Settlement | 8 num fijo |
| 10 | Conversion Rate, Cardholder Billing | 8 num fijo |
| 12 | Date and Time, Local Transaction | 12 num fijo |
| 22 | Point-of-Service Data Code | 12 an fijo |
| 23 | Card Sequence Number | 3 num fijo |
| 24 | Function Code | 3 num fijo |
| 26 | Card Acceptor Business Code (MCC) | 4 num fijo |
| 31 | Acquirer Reference Data | var, LLVAR |
| 33 | Forwarding Institution ID Code | var, LLVAR |
| 38 | Approval Code | 6 an fijo |
| **40** | **Service Restriction Code** | **3 num fijo** |
| 42 | Card Acceptor ID Code | 15 an fijo |
| 43 | Card Acceptor Name/Location | var, LLVAR |
| 48 | Additional Data — Private (PDS) | var, LLLVAR |
| 49 | Currency Code, Transaction | 3 num fijo |
| 50 | Currency Code, Settlement | 3 num fijo |
| 51 | Currency Code, Cardholder Billing | 3 num fijo |
| 55 | ICC Data (chip/EMV) | var, LLLVAR |
| 63 | Network Data | var, LLLVAR |
| 71 | Message Number | 8 num fijo |
| 93 | Transaction Destination Institution ID Code | var, LLVAR |
| 94 | Transaction Originator Institution ID Code | var, LLVAR |
| 100 | Receiving Institution ID Code | var, LLVAR |
| 105 | Large Data Elements | var, LLLVAR |

---

## Estado de DE 40 en el pipeline

**DE 40 — Service Restriction Code (3 bytes fijo)**

| Capa | Configurado | Detalle |
|------|:-----------:|---------|
| `lmbd-mc-interpreter` — `Parameters.getdataelements()` | ✓ | `40: {"fixed": True, "length": 3}` (handler.py:847) — el parser lo lee y lo incluye en el Parquet RAW |
| DynamoDB `mastercard_fields-02` | ✗ | No existe ningún item con `DE_40` (verificado 2026-06-24, 209 items totales) |
| `lmbd-mc-transform` → capas TRA/EXT/CLN/CAL/ITX | ✗ | Al no estar en DynamoDB, mc-transform no lo incluye; el valor se pierde desde la capa TRA en adelante |

**Impacto:** si un mensaje tiene DE 40 en el bitmap, el interpreter lo parsea correctamente (no falla, no desincroniza el stream), pero el valor **no llega a operational**. No genera error visible en CloudWatch — la pérdida es silenciosa.

**Para incluir DE 40 en el pipeline:** agregar un item en DynamoDB `mastercard_fields-02` con el mismo esquema que los DEs existentes del MTI 1240 (type_record, column_name, length, dtype, etc.) y ejecutar un reproceso desde `lmbd-mc-transform` en adelante para los archivos afectados.

---

## Notas para el revisor

- Los mensajes con estos bitmaps son **MTI 1240** (transaccionales principales): presencia de DE 24 (Function Code), DE 48 (PDS), DE 55 (EMV/chip) y DE 105 (Large Data) es característica de este MTI.
- La diferencia entre Bitmap A y Bitmap B es únicamente DE 40 — indica que algunos mensajes del archivo tienen Service Restriction Code y otros no; es normal que el bitmap varíe por mensaje dentro del mismo archivo.
- Para parsear bitmaps de otros mensajes del mismo cliente, usar el snippet de Python de la sección anterior o el script `tst_files/debug_scripts/` si ya existe uno de bitmap parsing en el repo.
- Código fuente del parser: `lambdas/mastercard/interpreter/src/handler.py` — funciones `bitmap_bits()` (línea 757) y `split_mti_bitmap_body()` (línea 772).

"""
logger.py

Módulo interno del pipeline IAR (Mastercard) — configura el logging estándar
de Python para todo el pipeline (formato con timestamp/nivel/logger/mensaje,
salida a stdout para que CloudWatch lo capture) y expone el logger
`pipeline_iar` ya listo para importar (`from logs.logger import logger`),
usado por `handler.py` y `raw.py`. El nivel se controla con la variable de
entorno `ITX_LOG_LEVEL` (default "INFO").
"""

import logging
import os
import sys

LOG_LEVEL = os.environ.get("ITX_LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ],
    force=True
)

logger = logging.getLogger("pipeline_iar")

class Logger:
    """
    Envoltorio delgado para obtener un logger con un nombre distinto al
    logger `pipeline_iar` por defecto — no usado actualmente por ningún
    módulo del pipeline IAR (`from logs.logger import logger` es la forma
    real de uso), reservado para casos donde se necesite un logger con
    nombre propio por módulo.
    """
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
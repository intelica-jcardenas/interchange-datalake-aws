"""
logger.py

Utilitario de logging del motor de reglas ARDEF de Visa. Provee una clase
`Logger` que envuelve `logging.getLogger` con configuración estándar
(formato, nivel, handlers) consistente en todos los módulos de `ardef`, con
comportamiento distinto según se ejecute en AWS Lambda o localmente.
"""

import logging
import logging.handlers
import os
from collections import OrderedDict

# Solo carga .env si estamos en local (no en Lamda)
running_in_lambda = "AWS_LAMBDA_FUNCTION_NAME" in os.environ
if not running_in_lambda:
    import dotenv
    dotenv.load_dotenv()


class Logger:
    """
    Provee un logger estandarizado para imprimir y persistir mensajes de log
    en todos los módulos de `ardef`, evitando handlers duplicados si ya se
    instanció un `Logger` con el mismo `name`.

    En Lambda: solo StreamHandler -> stdout -> CloudWatch Logs automáticamente.
    En local: StreamHandler -> FileHandler -> consola -> archivo de log en disco.
    """

    _LOG_LEVELS = OrderedDict(
        {
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "warning": logging.WARNING,
            "error": logging.ERROR,
            "critical": logging.CRITICAL,
        }
    )

    _DEFAULT_FMT = (
        "%(asctime)s :: PID %(process)d :: TID %(thread)d :: "
        "%(module)s.%(funcName)s :: Line %(lineno)d :: "
        "%(levelname)s :: %(message)s"
    )

    def __init__(self, name: str) -> None:
        """
        Obtiene (o crea) el logger de Python con el `name` indicado y le
        configura nivel + handlers una única vez. Si el logger ya tiene
        handlers (misma instancia reutilizada, ej. mismo `__name__` en
        distintos módulos), no vuelve a configurarlos — evita duplicar
        líneas de log.

        Configura siempre un StreamHandler (en Lambda, stdout es capturado
        automáticamente por CloudWatch Logs; en local, imprime en consola).
        Además, solo si NO se está ejecutando en Lambda (detectado por la
        ausencia de la variable de entorno AWS_LAMBDA_FUNCTION_NAME, que AWS
        inyecta automáticamente), agrega un `TimedRotatingFileHandler` que
        rota diariamente y conserva 3 backups.

        Args:
            name: nombre del logger, típicamente `__name__` del módulo que lo crea.

        Returns:
            None. Deja el logger configurado en `self.logger`.

        Ejemplo:
            log = Logger(__name__)
            log.logger.info("mensaje")
        """
        self.logger = logging.getLogger(name)

        if self.logger.handlers:
            return

        log_level = os.environ.get("ITX_LOG_LEVEL", "info")
        self.logger.setLevel(self._LOG_LEVELS[log_level])

        formatter = logging.Formatter(self._DEFAULT_FMT)

        # StreamHandler siempre activo
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        # FileHandler solo en local (cuando NO estamos en Lambda)
        running_in_lambda = "AWS_LAMBDA_FUNCTION_NAME" in os.environ

        if not running_in_lambda:
            log_path = os.environ.get("ITX_LOG_PATH", "ardef/logs/ardef.log")
            file_handler = logging.handlers.TimedRotatingFileHandler(
                filename=log_path,
                when='D',
                backupCount=3,
                encoding='utf-8',
                delay=True,
            )
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

"""
Tests basicos de funciones criticas del Agente Kowen.

Ejecutar:
    python -m unittest tests.test_core
    # o:
    python -m unittest discover tests
"""

import os
import sys
import time
import unittest
from unittest.mock import patch, MagicMock

# Permitir importar modulos del proyecto
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import operations


# ===== _parse_cash_from_comment =====

class TestParseCashFromComment(unittest.TestCase):
    """Parser de efectivo desde comentario POD driv.in."""

    def test_empty_returns_none(self):
        self.assertIsNone(operations._parse_cash_from_comment(""))
        self.assertIsNone(operations._parse_cash_from_comment(None))

    def test_plain_amount(self):
        self.assertEqual(operations._parse_cash_from_comment("pago 5990"), 5990)

    def test_with_dollar_sign(self):
        self.assertEqual(operations._parse_cash_from_comment("pago $5.000"), 5000)

    def test_with_thousands_separator(self):
        self.assertEqual(operations._parse_cash_from_comment("pago 5.000 efectivo"), 5000)
        self.assertEqual(operations._parse_cash_from_comment("pago 10,000 en efectivo"), 10000)

    def test_multiple_amounts_takes_max(self):
        # Si hay varios, toma el mayor (asumimos es el total)
        self.assertEqual(
            operations._parse_cash_from_comment("2 botellones a 2990 = 5980"),
            5980,
        )

    def test_ignores_transfer_mention(self):
        self.assertIsNone(operations._parse_cash_from_comment("transferencia 5000"))
        self.assertIsNone(operations._parse_cash_from_comment("pago 5000 webpay"))
        self.assertIsNone(operations._parse_cash_from_comment("deposito 10000"))

    def test_out_of_range_ignored(self):
        # Cash >500000 o <1000 se descarta (proteccion contra falsos positivos)
        self.assertIsNone(operations._parse_cash_from_comment("casa 123"))
        self.assertIsNone(operations._parse_cash_from_comment("999999999"))

    def test_text_only_returns_none(self):
        self.assertIsNone(operations._parse_cash_from_comment("gracias!"))
        self.assertIsNone(operations._parse_cash_from_comment("entregado ok"))


# ===== _sync_lock =====

class TestSyncLock(unittest.TestCase):
    """File-based lock para evitar ejecuciones concurrentes."""

    def setUp(self):
        # Limpiar locks previos
        self.lock_name = "test_sync_lock_unittest"
        self.lock_path = os.path.join(operations._LOCK_DIR, f"{self.lock_name}.lock")
        if os.path.exists(self.lock_path):
            os.remove(self.lock_path)

    def tearDown(self):
        if os.path.exists(self.lock_path):
            os.remove(self.lock_path)

    def test_acquire_and_release(self):
        with operations._sync_lock(self.lock_name) as acquired:
            self.assertTrue(acquired)
            self.assertTrue(os.path.exists(self.lock_path))
        # Al salir del contexto, el lock se elimina
        self.assertFalse(os.path.exists(self.lock_path))

    def test_reentrant_blocks(self):
        # Tomar el lock y mientras esta tomado, otro intento falla
        with operations._sync_lock(self.lock_name) as first:
            self.assertTrue(first)
            with operations._sync_lock(self.lock_name) as second:
                self.assertFalse(second)
                # El archivo sigue existiendo (no lo borra el segundo)
                self.assertTrue(os.path.exists(self.lock_path))

    def test_stale_lock_reclaimed(self):
        # Crear lock "viejo" (mas de 5 min) manualmente
        os.makedirs(operations._LOCK_DIR, exist_ok=True)
        with open(self.lock_path, "w") as f:
            f.write("old")
        old_time = time.time() - (operations._LOCK_STALE_SECONDS + 10)
        os.utime(self.lock_path, (old_time, old_time))

        # El sync_lock debe reclamarlo
        with operations._sync_lock(self.lock_name) as acquired:
            self.assertTrue(acquired)


# ===== reconciliar_pagos =====

class TestReconciliarPagos(unittest.TestCase):
    """Sincroniza PAGOS -> OPERACION DIARIA."""

    @patch("operations.update_pedidos_batch")
    def test_actualiza_pedido_no_pagado(self, mock_batch):
        pagos = [{
            "Fecha": "20/04/2026",
            "Monto": "5990",
            "Medio": "Transferencia",
            "Pedido Vinculado": "42",
            "Estado": "CONCILIADO_MANUAL",
        }]
        pedidos = [{
            "#": "42",
            "Fecha": "20/04/2026",
            "Cliente": "Juan Perez",
            "Estado Pago": "PENDIENTE",
        }]
        with patch("sheets_client.get_pagos", return_value=pagos), \
             patch("sheets_client.get_pedidos", return_value=pedidos):
            r = operations.reconciliar_pagos()

        self.assertEqual(r["actualizados"], 1)
        self.assertEqual(len(r["huerfanos"]), 0)
        mock_batch.assert_called_once()
        call_args = mock_batch.call_args[0][0]
        self.assertEqual(call_args[0][0], 42)
        self.assertEqual(call_args[0][1]["estado_pago"], "PAGADO")
        self.assertEqual(call_args[0][1]["forma_pago"], "Transferencia")

    @patch("operations.update_pedidos_batch")
    def test_no_duplica_si_ya_pagado(self, mock_batch):
        pagos = [{
            "Fecha": "20/04/2026",
            "Pedido Vinculado": "42",
            "Estado": "CONCILIADO_MANUAL",
            "Medio": "Transferencia",
        }]
        pedidos = [{
            "#": "42",
            "Estado Pago": "PAGADO",
            "Cliente": "Juan",
        }]
        with patch("sheets_client.get_pagos", return_value=pagos), \
             patch("sheets_client.get_pedidos", return_value=pedidos):
            r = operations.reconciliar_pagos()

        self.assertEqual(r["actualizados"], 0)
        mock_batch.assert_not_called()

    @patch("operations.update_pedidos_batch")
    def test_detecta_huerfano(self, mock_batch):
        # Pedido PAGADO en OPERACION DIARIA pero sin fila en PAGOS
        with patch("sheets_client.get_pagos", return_value=[]), \
             patch("sheets_client.get_pedidos", return_value=[{
                "#": "99",
                "Estado Pago": "PAGADO",
                "Cliente": "Maria",
                "Fecha": "20/04/2026",
                "Forma Pago": "Efectivo",
             }]):
            r = operations.reconciliar_pagos()

        self.assertEqual(len(r["huerfanos"]), 1)
        self.assertEqual(r["huerfanos"][0]["numero"], 99)

    @patch("operations.update_pedidos_batch")
    def test_detecta_pago_sin_pedido(self, mock_batch):
        pagos = [{
            "Fecha": "20/04/2026",
            "Monto": "5990",
            "Pedido Vinculado": "777",
            "Estado": "CONCILIADO_MANUAL",
            "Medio": "Transferencia",
        }]
        with patch("sheets_client.get_pagos", return_value=pagos), \
             patch("sheets_client.get_pedidos", return_value=[]):
            r = operations.reconciliar_pagos()

        self.assertEqual(len(r["sin_pedido"]), 1)
        self.assertEqual(r["sin_pedido"][0]["pedido_num"], 777)


# ===== sync_clientes_from_operacion =====

class TestSyncClientesFromOperacion(unittest.TestCase):
    """Deriva tab CLIENTES desde OPERACION DIARIA."""

    @patch("sheets_client.update_cliente")
    @patch("sheets_client.add_cliente")
    def test_crea_cliente_nuevo(self, mock_add, mock_update):
        pedidos = [
            {"#": "1", "Fecha": "18/04/2026", "Codigo Drivin": "CL-001",
             "Cliente": "Juan Perez", "Direccion": "Calle 123",
             "Depto": "", "Comuna": "Las Condes", "Telefono": "+56911",
             "Email": "j@p.cl", "Marca": "KOWEN"},
            {"#": "2", "Fecha": "20/04/2026", "Codigo Drivin": "CL-001",
             "Cliente": "Juan Perez", "Direccion": "Calle 123",
             "Depto": "", "Comuna": "Las Condes", "Telefono": "+56911",
             "Email": "j@p.cl", "Marca": "KOWEN"},
        ]
        with patch("operations.get_pedidos", return_value=pedidos), \
             patch("sheets_client.get_clientes", return_value=[]):
            r = operations.sync_clientes_from_operacion()

        self.assertEqual(r["creados"], 1)
        self.assertEqual(r["actualizados"], 0)
        mock_add.assert_called_once()
        called = mock_add.call_args[0][0]
        self.assertEqual(called["codigo_drivin"], "CL-001")
        self.assertEqual(called["total_pedidos"], 2)
        self.assertEqual(called["ultimo_pedido"], "20/04/2026")

    @patch("sheets_client.update_cliente")
    @patch("sheets_client.add_cliente")
    def test_actualiza_cliente_existente(self, mock_add, mock_update):
        pedidos = [{
            "#": "1", "Fecha": "20/04/2026", "Codigo Drivin": "CL-001",
            "Cliente": "Juan Perez", "Direccion": "Calle 123 Nueva",
            "Depto": "", "Comuna": "Las Condes", "Telefono": "+56922",
            "Email": "", "Marca": "KOWEN",
        }]
        existentes = [{
            "Nombre": "Juan Perez", "Codigo Drivin": "CL-001",
            "Direccion": "Calle 123 Vieja", "Telefono": "+56911",
            "Marca": "KOWEN", "Total Pedidos": "0", "Ultimo Pedido": "",
            "Comuna": "Las Condes", "Depto": "", "Email": "",
        }]
        with patch("operations.get_pedidos", return_value=pedidos), \
             patch("sheets_client.get_clientes", return_value=existentes):
            r = operations.sync_clientes_from_operacion()

        self.assertEqual(r["creados"], 0)
        self.assertEqual(r["actualizados"], 1)
        mock_add.assert_not_called()
        mock_update.assert_called_once()
        # Verificar que los campos cambiados estan en el update
        nombre_arg, updates_arg = mock_update.call_args[0]
        self.assertEqual(nombre_arg, "Juan Perez")
        self.assertIn("direccion", updates_arg)
        self.assertEqual(updates_arg["direccion"], "Calle 123 Nueva")
        self.assertEqual(updates_arg["telefono"], "+56922")

    @patch("sheets_client.update_cliente")
    @patch("sheets_client.add_cliente")
    def test_ignora_pedido_sin_nombre(self, mock_add, mock_update):
        pedidos = [{
            "#": "1", "Fecha": "20/04/2026", "Codigo Drivin": "",
            "Cliente": "",  # Sin nombre
            "Direccion": "Calle 123", "Depto": "", "Comuna": "",
            "Telefono": "", "Email": "", "Marca": "KOWEN",
        }]
        with patch("operations.get_pedidos", return_value=pedidos), \
             patch("sheets_client.get_clientes", return_value=[]):
            r = operations.sync_clientes_from_operacion()

        self.assertEqual(r["creados"], 0)
        mock_add.assert_not_called()


# ===== diagnostico_salud =====

class TestDiagnosticoSalud(unittest.TestCase):
    """Snapshot read-only de cosas que requieren atencion humana."""

    def test_detecta_huerfano(self):
        pedidos = [{
            "#": "10", "Fecha": "20/04/2026", "Estado Pedido": "ENTREGADO",
            "Estado Pago": "PAGADO", "Cliente": "X", "Forma Pago": "Efectivo",
            "Codigo Drivin": "CL-01", "Direccion": "Calle 1",
            "Transferencia": "", "Efectivo": "5990", "Comuna": "",
        }]
        with patch("sheets_client.get_pedidos", return_value=pedidos), \
             patch("sheets_client.get_pagos", return_value=[]):
            r = operations.diagnostico_salud()

        self.assertEqual(len(r["huerfanos"]), 1)
        self.assertEqual(r["huerfanos"][0]["numero"], 10)

    def test_detecta_estancado(self):
        # Pedido PENDIENTE de hace 5 dias
        from datetime import timedelta as _td
        fecha_vieja = (operations.config.now().replace(tzinfo=None) - _td(days=5)).strftime("%d/%m/%Y")
        pedidos = [{
            "#": "20", "Fecha": fecha_vieja, "Estado Pedido": "PENDIENTE",
            "Estado Pago": "", "Cliente": "Y", "Codigo Drivin": "CL-02",
            "Direccion": "Calle 2", "Comuna": "",
        }]
        with patch("sheets_client.get_pedidos", return_value=pedidos), \
             patch("sheets_client.get_pagos", return_value=[]):
            r = operations.diagnostico_salud(dias_estancado=2)

        self.assertEqual(len(r["estancados"]), 1)
        self.assertEqual(r["estancados"][0]["numero"], 20)
        self.assertGreaterEqual(r["estancados"][0]["dias"], 5)

    def test_detecta_pendiente_sin_codigo(self):
        hoy = operations.config.now().strftime("%d/%m/%Y")
        pedidos = [{
            "#": "30", "Fecha": hoy, "Estado Pedido": "PENDIENTE",
            "Estado Pago": "", "Cliente": "Z", "Codigo Drivin": "",
            "Direccion": "Calle 3", "Comuna": "Las Condes",
        }]
        with patch("sheets_client.get_pedidos", return_value=pedidos), \
             patch("sheets_client.get_pagos", return_value=[]):
            r = operations.diagnostico_salud()

        self.assertEqual(len(r["pendientes_sin_codigo"]), 1)
        self.assertEqual(r["pendientes_sin_codigo"][0]["numero"], 30)

    def test_no_marca_huerfano_si_hay_fila_pagos(self):
        pedidos = [{
            "#": "40", "Fecha": "20/04/2026", "Estado Pedido": "ENTREGADO",
            "Estado Pago": "PAGADO", "Cliente": "W", "Codigo Drivin": "CL-04",
            "Direccion": "Calle 4", "Comuna": "", "Forma Pago": "Transferencia",
        }]
        pagos = [{
            "Pedido Vinculado": "40", "Estado": "CONCILIADO_MANUAL",
            "Monto": "5990", "Fecha": "20/04/2026",
        }]
        with patch("sheets_client.get_pedidos", return_value=pedidos), \
             patch("sheets_client.get_pagos", return_value=pagos):
            r = operations.diagnostico_salud()

        self.assertEqual(len(r["huerfanos"]), 0)

    def test_detecta_pago_sin_pedido(self):
        pagos = [{
            "Pedido Vinculado": "999", "Estado": "CONCILIADO_AUTO",
            "Monto": "5990", "Fecha": "20/04/2026", "Cliente": "fantasma",
        }]
        with patch("sheets_client.get_pedidos", return_value=[]), \
             patch("sheets_client.get_pagos", return_value=pagos):
            r = operations.diagnostico_salud()

        self.assertEqual(len(r["pagos_sin_pedido"]), 1)
        self.assertEqual(r["pagos_sin_pedido"][0]["pedido_num"], 999)


if __name__ == "__main__":
    unittest.main()

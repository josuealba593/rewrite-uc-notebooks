"""
app.py — Rewrite UC Notebooks | Marathon / Superdeporte
Streamlit app para procesar notebooks .ipynb exportados desde Databricks
y aplicar los cambios necesarios para Unity Catalog.
"""

import re
import json
import copy
import zipfile
import io
from datetime import datetime
from pathlib import Path

import streamlit as st

# ═══════════════════════════════════════════════════════════════════
# CONFIGURACIÓN
# ═══════════════════════════════════════════════════════════════════

CATALOG = "regional"
UC_MARKER = "# UC-MIGRATION: USE CATALOG"

MARATHON_SCHEMAS = [
    "db_bronze_ec", "db_silver_ec", "db_gold", "db_silver", "db_bronze",
    "db_silver_pe", "db_bronze_pe", "db_nrt", "db_erp", "db_tmp",
    "db_bing", "db_datascience", "db_parameters", "db_teradata",
    "db_bronze_cl", "db_silver_cl", "db_logs", "default", "db_dq",
]
SCH = r'(?:' + '|'.join(MARATHON_SCHEMAS) + r')'

MNT_MAPPINGS = {
    "/mnt/bronze/": "s3://s3-marathon-bronze-noproductivo/",
    "/mnt/silver/": "s3://s3-marathon-silver-noproductivo/",
    "/mnt/gold/": "s3://s3-marathon-gold-noproductivo/",
    "dbfs:/mnt/bronze/": "s3://s3-marathon-bronze-noproductivo/",
    "dbfs:/mnt/silver/": "s3://s3-marathon-silver-noproductivo/",
    "dbfs:/mnt/gold/": "s3://s3-marathon-gold-noproductivo/",
    "/dbfs/mnt/bronze/": "s3://s3-marathon-bronze-noproductivo/",
    "/dbfs/mnt/silver/": "s3://s3-marathon-silver-noproductivo/",
    "/dbfs/mnt/gold/": "s3://s3-marathon-gold-noproductivo/",
}

# Flags que requieren revisión manual
# P14 es informativo — no genera alerta de acción requerida
FLAGS_INFO = {
    "P9": {
        "titulo": "Widget `database_source` sin `catalog_source`",
        "requiere_accion": True,
        "instruccion": (
            "El notebook recibe el nombre de la base de datos como parámetro (`database_source`). "
            "En Unity Catalog también necesita recibir el catálogo.\n\n"
            "**El script agregó automáticamente el widget `catalog_source`**, pero las consultas "
            "que usan esa variable deben actualizarse manualmente.\n\n"
            "**Qué buscar en el notebook:**\n"
            "Líneas con patrones como:\n"
            "```\n"
            "spark.sql(f\"SELECT * FROM {database}.mi_tabla\")\n"
            "spark.table(f\"{database}.mi_tabla\")\n"
            "```\n"
            "**Cómo corregirlas:**\n"
            "```\n"
            "# ANTES\n"
            "spark.sql(f\"SELECT * FROM {database}.mi_tabla\")\n"
            "# DESPUÉS\n"
            "spark.sql(f\"SELECT * FROM {catalog}.{database}.mi_tabla\")\n"
            "```\n"
            "Donde `catalog = dbutils.widgets.get('catalog_source')`."
        ),
    },
    "P13": {
        "titulo": "Ruta `/mnt/` que no puede reemplazarse automáticamente",
        "requiere_accion": True,
        "instruccion": (
            "Esta ruta apunta a una ubicación en storage que no está en el mapeo estándar "
            "o es una LOCATION de tabla Delta que no debe modificarse automáticamente.\n\n"
            "**Qué hacer:**\n"
            "Consulta con el equipo de Prediqt cuál es la ruta S3 correcta para reemplazarla. "
            "Una vez confirmada, edita la línea indicada directamente en el notebook."
        ),
    },
    "P14": {
        "titulo": "Sentencia GRANT/REVOKE detectada",
        "requiere_accion": False,
        "instruccion": (
            "Estas sentencias son permisos de **Redshift**, no de Databricks. "
            "**No deben modificarse.** El script las dejó intactas."
        ),
    },
    "P17": {
        "titulo": "Uso de `call_engine` o `ingest_parameters`",
        "requiere_accion": False,
        "instruccion": (
            "Este notebook usa el motor central de ingesta. "
            "El `USE CATALOG` insertado al inicio es suficiente para que funcione correctamente. "
            "Verifica en la primera ejecución real que no haya errores de catálogo."
        ),
    },
    "P23": {
        "titulo": "Consulta con variable dinámica `{database}.{tabla}`",
        "requiere_accion": True,
        "instruccion": (
            "El notebook construye consultas SQL usando variables para el nombre de la base de datos. "
            "En Unity Catalog las consultas deben incluir también el catálogo.\n\n"
            "**Qué buscar:** Las líneas indicadas abajo contienen patrones como "
            "`{database}.tabla` o `{db}.tabla`.\n\n"
            "**Cómo corregirlas:**\n"
            "```\n"
            "# ANTES\n"
            "spark.sql(f\"SELECT * FROM {database}.mi_tabla\")\n"
            "# DESPUÉS\n"
            "spark.sql(f\"SELECT * FROM regional.{database}.mi_tabla\")\n"
            "```\n"
            "Si el nombre del catálogo puede variar, usa una variable:\n"
            "```\n"
            "catalog = 'regional'\n"
            "spark.sql(f\"SELECT * FROM {catalog}.{database}.mi_tabla\")\n"
            "```"
        ),
    },
    "P99": {
        "titulo": "`hive_metastore.` como texto dentro de un string",
        "requiere_accion": True,
        "instruccion": (
            "El notebook contiene el texto `hive_metastore.` dentro de un string "
            "(no como referencia directa a una tabla). Puede ser lógica de limpieza o logging.\n\n"
            "**Qué hacer:** Revisa si esa lógica sigue siendo necesaria en Unity Catalog. "
            "Si el código elimina prefijos `hive_metastore.` de strings, probablemente ya no "
            "sea necesario y puedes eliminarlo o dejarlo como está."
        ),
    },
    "P_S3A": {
        "titulo": "Escritura/lectura directa a bucket S3 externo (`s3a://`)",
        "requiere_accion": False,
        "instruccion": (
            "Este notebook accede directamente a un bucket S3 usando el protocolo `s3a://`. "
            "Estas rutas no pasan por los montajes estándar `/mnt/` y no fueron modificadas automáticamente.\n\n"
            "**Qué verificar antes de ejecutar en el nuevo workspace:**\n"
            "Confirma con Agustín que el nuevo workspace tiene acceso a ese bucket. "
            "En Unity Catalog el acceso a S3 externo requiere una **External Location** configurada "
            "por el administrador — no basta con el instance profile del workspace viejo.\n\n"
            "Si el bucket no tiene External Location configurada, el notebook fallará al intentar "
            "leer o escribir aunque el resto del código esté correcto."
        ),
    },
}


# ═══════════════════════════════════════════════════════════════════
# UTILIDADES ipynb
# ═══════════════════════════════════════════════════════════════════

def source_to_text(source) -> str:
    if isinstance(source, list):
        return "".join(source)
    return source if isinstance(source, str) else ""


def set_cell_source(cell: dict, texto: str) -> None:
    lineas = texto.split("\n")
    cell["source"] = [
        l + ("\n" if i < len(lineas) - 1 else "")
        for i, l in enumerate(lineas)
    ]


def texto_completo_notebook(nb: dict) -> str:
    return "\n".join(
        source_to_text(c.get("source", []))
        for c in nb.get("cells", [])
        if c.get("cell_type") == "code"
    )


# ═══════════════════════════════════════════════════════════════════
# TRANSFORMACIONES
# ═══════════════════════════════════════════════════════════════════

def insertar_use_catalog(nb: dict) -> tuple:
    texto = texto_completo_notebook(nb)
    ya_tiene = (
        UC_MARKER in texto
        or bool(re.search(rf'use\s+catalog\s+{re.escape(CATALOG)}', texto, re.I))
    )
    if ya_tiene:
        return False, "Ya tenía `USE CATALOG regional` — no se duplicó."

    nueva_celda = {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            UC_MARKER + "\n",
            f'spark.sql("USE CATALOG {CATALOG}")',
        ],
    }
    cells = nb.get("cells", [])
    idx = next((i for i, c in enumerate(cells) if c.get("cell_type") == "code"), 0)
    nb["cells"].insert(idx, nueva_celda)
    return True, f"Insertado al inicio del notebook (celda {idx + 1})."


def agregar_widget_catalog_source(nb: dict) -> tuple:
    """
    Agrega widget catalog_source junto a database_source si no existe.
    Retorna también las líneas con variables dinámicas que usan database_source
    y que deben actualizarse manualmente en las consultas SQL.
    """
    patron_widget = re.compile(
        r'(dbutils\.widgets\.(text|combobox|dropdown)\s*\(\s*["\']database_source["\'][^\n]*)',
        re.I
    )
    patron_ya_tiene = re.compile(r'catalog_source', re.I)
    texto_nb = texto_completo_notebook(nb)

    if not patron_widget.search(texto_nb):
        return False, None, []
    if patron_ya_tiene.search(texto_nb):
        return False, "ya tenía catalog_source", []

    # Extraer nombre de la variable que recibe database_source
    # Ejemplo: database = dbutils.widgets.get("database_source") -> variable = "database"
    patron_get = re.compile(
        r'(\w+)\s*=\s*dbutils\.widgets\.get\s*\(\s*["\']database_source["\']\s*\)',
        re.I
    )
    variables_db = set()
    for cell in nb.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        src = source_to_text(cell.get("source", []))
        for m in patron_get.finditer(src):
            variables_db.add(m.group(1))

    # Detectar líneas con consultas que usan esas variables dinámicamente
    lineas_dinamicas = []
    if variables_db:
        patron_dinamico = re.compile(
            r'\{(' + '|'.join(re.escape(v) for v in variables_db) + r')\}',
            re.I
        )
        for num_celda, cell in enumerate(nb.get("cells", []), start=1):
            if cell.get("cell_type") != "code":
                continue
            src = source_to_text(cell.get("source", []))
            for num_linea, linea in enumerate(src.split("\n"), start=1):
                if patron_dinamico.search(linea) and linea.strip() and not linea.strip().startswith("#"):
                    lineas_dinamicas.append({
                        "celda": num_celda,
                        "linea": num_linea,
                        "codigo": linea.strip(),
                    })

    # Agregar widget catalog_source
    insertados = 0
    for cell in nb.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        src = source_to_text(cell.get("source", []))
        if not patron_widget.search(src):
            continue
        lineas = src.split("\n")
        nuevas = []
        for linea in lineas:
            nuevas.append(linea)
            m = patron_widget.search(linea)
            if m:
                indent = len(linea) - len(linea.lstrip())
                if indent == 0:
                    nuevas.append('dbutils.widgets.text("catalog_source", "regional")')
                    insertados += 1
        set_cell_source(cell, "\n".join(nuevas))

    return insertados > 0, "widget `catalog_source` agregado automáticamente.", lineas_dinamicas


def limpiar_hms(nb: dict) -> tuple:
    patron = re.compile(r'hive_metastore\.(' + SCH + r'\.)', re.I)
    total = 0
    cambios = []
    for num_celda, cell in enumerate(nb.get("cells", []), start=1):
        if cell.get("cell_type") != "code":
            continue
        src = source_to_text(cell.get("source", []))
        for i, linea in enumerate(src.split("\n"), start=1):
            if patron.search(linea):
                cambios.append({
                    "celda": num_celda, "linea": i,
                    "antes": linea.strip(),
                    "despues": patron.sub(r'\1', linea).strip(),
                })
        src_nuevo, n = patron.subn(r'\1', src)
        if n > 0:
            total += n
            set_cell_source(cell, src_nuevo)
    return total, cambios


def clasificar_p13(linea: str) -> str:
    if linea.strip().startswith(("#", "--")):
        return "COMENTARIO"
    if re.search(
        r'\bLOCATION\s+["\'](?:dbfs:)?/?mnt/(?:bronze|silver|gold)/[^"\']*db_[\w]+\.db\b',
        linea, re.I
    ):
        return "NO_AUTOMATIZAR"
    if re.search(
        r'(?:(?:dbfs:)?/mnt|/dbfs/mnt)/(?:bronze|silver|gold)/db_[\w]+\.db\b',
        linea, re.I
    ):
        return "NO_AUTOMATIZAR"
    if re.search(
        r'(?:(?:dbfs:)?/mnt|/dbfs/mnt)/bronze/'
        r'(?:static_files|tmp|ingestion_parameters|ingestion_parameters_ERP)\b',
        linea, re.I
    ):
        return "AUTOMATIZABLE"
    return "REVISAR"


def reemplazar_mnt(nb: dict) -> tuple:
    mappings = sorted(MNT_MAPPINGS.items(), key=lambda x: len(x[0]), reverse=True)
    total = 0
    cambios = []
    for num_celda, cell in enumerate(nb.get("cells", []), start=1):
        if cell.get("cell_type") != "code":
            continue
        src = source_to_text(cell.get("source", []))
        lineas = src.split("\n")
        modificado = False
        for i, linea in enumerate(lineas):
            if clasificar_p13(linea) != "AUTOMATIZABLE":
                continue
            orig = linea
            for mnt, s3 in mappings:
                if mnt in linea:
                    linea = linea.replace(mnt, s3)
            if linea != orig:
                cambios.append({
                    "celda": num_celda, "linea": i + 1,
                    "antes": orig.strip(), "despues": linea.strip(),
                })
                total += 1
                modificado = True
            lineas[i] = linea
        if modificado:
            set_cell_source(cell, "\n".join(lineas))
    return total, cambios


PATRONES_FLAGS = {
    "P13": re.compile(r'''['"/ ](?:dbfs:)?/mnt/\w''', re.I),
    "P14": re.compile(r'\b(?:GRANT|REVOKE|ALTER\s+DEFAULT\s+PRIVILEGES)\b', re.I),
    "P17": re.compile(r'\b(call_engine|ingest_parameters)\b', re.I),
    "P23": re.compile(
        r'\{(\w*(?:database|schema|db)\w*)\}\s*\.\s*\{(\w*(?:table|tabla)\w*)\}', re.I
    ),
    "P99": re.compile(r'''["']hive_metastore\.["']''', re.I),
    "P_S3A": re.compile(r's3a?://(?!s3-marathon-bronze|s3-marathon-silver|s3-marathon-gold)([a-zA-Z0-9_\-]+)/', re.I),
}


def detectar_flags(nb: dict) -> dict:
    encontrados = {}
    for num_celda, cell in enumerate(nb.get("cells", []), start=1):
        if cell.get("cell_type") != "code":
            continue
        src = source_to_text(cell.get("source", []))
        for num_linea, linea in enumerate(src.split("\n"), start=1):
            for flag_id, patron in PATRONES_FLAGS.items():
                if not patron.search(linea):
                    continue
                if flag_id == "P13" and clasificar_p13(linea) == "COMENTARIO":
                    continue
                if flag_id not in encontrados:
                    encontrados[flag_id] = []
                encontrados[flag_id].append({
                    "celda": num_celda,
                    "linea": num_linea,
                    "codigo": linea.strip(),
                })
    return encontrados


# ═══════════════════════════════════════════════════════════════════
# PROCESADOR PRINCIPAL
# ═══════════════════════════════════════════════════════════════════

def procesar_notebook(nombre: str, contenido_bytes: bytes) -> dict:
    """
    Retorna dict con:
      - nombre_original
      - bytes_original
      - bytes_corregido
      - estado: 'ok' | 'revision' | 'error'
      - cambios: dict con detalle de cada transformación
      - flags: dict con flags residuales
      - error_msg: str si estado == 'error'
    """
    try:
        raw = contenido_bytes.decode("utf-8-sig", errors="replace")
        nb = json.loads(raw)
    except Exception as e:
        return {
            "nombre_original": nombre,
            "bytes_original": contenido_bytes,
            "bytes_corregido": None,
            "estado": "error",
            "cambios": {},
            "flags": {},
            "error_msg": (
                f"No se pudo leer el archivo: {e}\n\n"
                "Asegúrate de exportarlo como **IPython Notebook (.ipynb)** desde "
                "File → Export en Databricks, no como DBC Archive."
            ),
        }

    if "nbformat" not in nb:
        return {
            "nombre_original": nombre,
            "bytes_original": contenido_bytes,
            "bytes_corregido": None,
            "estado": "error",
            "cambios": {},
            "flags": {},
            "error_msg": (
                "El archivo no es un notebook `.ipynb` válido.\n\n"
                "Asegúrate de exportarlo como **IPython Notebook (.ipynb)** desde "
                "File → Export en Databricks, no como DBC Archive."
            ),
        }

    nb_trabajo = copy.deepcopy(nb)

    # Conteos sobre el original
    texto_orig = texto_completo_notebook(nb_trabajo)
    hms_count = len(re.findall(r'hive_metastore\.' + SCH + r'\b', texto_orig, re.I))
    mnt_count = len(re.findall(r'''['"/ ](?:dbfs:)?/mnt/\w''', texto_orig, re.I))

    # Aplicar transformaciones
    uc_insertado, uc_msg           = insertar_use_catalog(nb_trabajo)
    p9_insertado, p9_msg, p9_lineas_dinamicas = agregar_widget_catalog_source(nb_trabajo)
    hms_eliminados, cambios_hms    = limpiar_hms(nb_trabajo)
    mnt_reemplazados, cambios_mnt  = reemplazar_mnt(nb_trabajo)

    # Detectar flags RESIDUALES (sobre notebook ya transformado)
    flags = detectar_flags(nb_trabajo)

    # Determinar estado
    flags_accion = {k: v for k, v in flags.items() if FLAGS_INFO.get(k, {}).get("requiere_accion")}
    estado = "revision" if flags_accion else "ok"

    return {
        "nombre_original": nombre,
        "bytes_original": contenido_bytes,
        "bytes_corregido": json.dumps(nb_trabajo, indent=1, ensure_ascii=False).encode("utf-8"),
        "estado": estado,
        "cambios": {
            "uc_insertado": uc_insertado,
            "uc_msg": uc_msg,
            "p9_insertado": p9_insertado,
            "p9_msg": p9_msg,
            "p9_lineas_dinamicas": p9_lineas_dinamicas,
            "hms_count": hms_count,
            "hms_eliminados": hms_eliminados,
            "cambios_hms": cambios_hms,
            "mnt_count": mnt_count,
            "mnt_reemplazados": mnt_reemplazados,
            "cambios_mnt": cambios_mnt,
        },
        "flags": flags,
        "error_msg": None,
    }


# ═══════════════════════════════════════════════════════════════════
# RENDERIZADO DE RESULTADOS
# ═══════════════════════════════════════════════════════════════════

def render_resultado(r: dict):
    nombre = r["nombre_original"]
    stem = Path(nombre).stem

    if r["estado"] == "error":
        st.error(f"❌ **{nombre}** — No se pudo procesar")
        with st.expander("Ver detalle del error"):
            st.markdown(r["error_msg"])
        return

    c = r["cambios"]
    flags = r["flags"]
    flags_accion = {k: v for k, v in flags.items() if FLAGS_INFO.get(k, {}).get("requiere_accion")}
    flags_info   = {k: v for k, v in flags.items() if not FLAGS_INFO.get(k, {}).get("requiere_accion")}

    # ── Encabezado estado ──────────────────────────────────────────
    if r["estado"] == "ok":
        st.success(f"✅ **{nombre}** — Procesado correctamente")
        st.caption("No se requiere ninguna acción adicional. Descarga el archivo corregido y súbelo al nuevo workspace.")
    else:
        st.warning(f"⚠️ **{nombre}** — Procesado con observaciones")
        st.caption("El archivo fue corregido automáticamente, pero hay puntos que requieren revisión manual antes de subirlo al nuevo workspace.")

    with st.expander("Ver detalle completo", expanded=(r["estado"] == "revision")):

        # ── Cambios automáticos ────────────────────────────────────
        st.markdown("#### ✅ Cambios aplicados automáticamente")

        col1, col2, col3 = st.columns(3)
        col1.metric("hive_metastore. eliminados", f"{c['hms_eliminados']} / {c['hms_count']}")
        col2.metric("/mnt/ reemplazados", f"{c['mnt_reemplazados']} / {c['mnt_count']}")
        col3.metric("USE CATALOG", "Insertado" if c["uc_insertado"] else "Ya existía")

        if c["uc_insertado"]:
            st.markdown(f"- 🟢 `USE CATALOG regional` insertado al inicio del notebook.")
        else:
            st.markdown(f"- ℹ️ `USE CATALOG regional`: {c['uc_msg']}")

        if c["p9_insertado"]:
            st.markdown("- 🟢 Widget `catalog_source` agregado automáticamente junto a `database_source`.")
            if c.get("p9_lineas_dinamicas"):
                st.warning(
                    f"⚠️ Se detectaron **{len(c['p9_lineas_dinamicas'])} consulta(s)** que usan la variable "
                    f"`database_source` dinámicamente y deben actualizarse a mano."
                )
                with st.expander("Ver líneas que requieren actualización manual"):
                    st.markdown(
                        "Estas líneas construyen consultas con `{database}` o similar. "
                        "Deben incluir el catálogo: `{catalog}.{database}.tabla`"
                    )
                    for ld in c["p9_lineas_dinamicas"]:
                        st.code(
                            f"Celda {ld['celda']}, línea {ld['linea']}:\n  {ld['codigo']}",
                            language="python"
                        )

        if c["cambios_hms"]:
            with st.expander(f"Ver {len(c['cambios_hms'])} cambios de hive_metastore."):
                for ch in c["cambios_hms"]:
                    st.code(
                        f"Celda {ch['celda']}, línea {ch['linea']}\n"
                        f"  ANTES:   {ch['antes']}\n"
                        f"  DESPUÉS: {ch['despues']}",
                        language="diff"
                    )

        if c["cambios_mnt"]:
            with st.expander(f"Ver {len(c['cambios_mnt'])} cambios de /mnt/ → S3"):
                for cm in c["cambios_mnt"]:
                    st.code(
                        f"Celda {cm['celda']}, línea {cm['linea']}\n"
                        f"  ANTES:   {cm['antes']}\n"
                        f"  DESPUÉS: {cm['despues']}",
                        language="diff"
                    )

        # ── Flags que requieren acción ─────────────────────────────
        if flags_accion:
            st.markdown("---")
            st.markdown("#### ⚠️ Requiere intervención humana antes de subir al nuevo workspace")

            for flag_id, ocurrencias in flags_accion.items():
                info = FLAGS_INFO[flag_id]
                st.markdown(f"**[{flag_id}] {info['titulo']}**")
                st.markdown(info["instruccion"])
                st.markdown(f"**Líneas afectadas ({len(ocurrencias)}):**")
                for oc in ocurrencias:
                    st.code(
                        f"Celda {oc['celda']}, línea {oc['linea']}:\n  {oc['codigo']}",
                        language="python"
                    )
                st.markdown("---")

        # ── Flags informativos (sin acción requerida) ──────────────
        if flags_info:
            with st.expander("ℹ️ Observaciones informativas (no requieren acción)"):
                for flag_id, ocurrencias in flags_info.items():
                    info = FLAGS_INFO[flag_id]
                    st.markdown(f"**[{flag_id}] {info['titulo']}**")
                    st.markdown(info["instruccion"])
                    for oc in ocurrencias:
                        st.code(
                            f"Celda {oc['celda']}, línea {oc['linea']}:\n  {oc['codigo']}",
                            language="python"
                        )


# ═══════════════════════════════════════════════════════════════════
# GENERACIÓN DE ARCHIVOS DE DESCARGA
# ═══════════════════════════════════════════════════════════════════

def generar_zip(resultados: list) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for r in resultados:
            stem = Path(r["nombre_original"]).stem
            nombre_orig = f"{stem}(ORIG).ipynb"
            nombre_corr = r["nombre_original"]

            zf.writestr(f"originales/{nombre_orig}", r["bytes_original"])
            if r["bytes_corregido"]:
                zf.writestr(f"corregidos/{nombre_corr}", r["bytes_corregido"])

            # Reporte txt
            reporte = generar_reporte_md(r)
            zf.writestr(f"reportes/reporte_{stem}.md", reporte)

    buf.seek(0)
    return buf.read()


def generar_reporte_md(r: dict) -> str:
    """Genera reporte en formato Markdown — legible y descargable."""
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M")
    nombre = r["nombre_original"]
    lines = [
        f"# Reporte Rewrite UC — `{nombre}`",
        f"**Fecha:** {fecha}  ",
        "",
    ]

    # ── Estado principal ──────────────────────────────────────────
    if r["estado"] == "error":
        lines += [
            "## ❌ No se pudo procesar",
            "",
            f"> {r['error_msg']}",
            "",
            "---",
            "Asegúrate de exportar el notebook como **IPython Notebook (.ipynb)** desde",
            "**File → Export** en Databricks, no como DBC Archive.",
        ]
        return "\n".join(lines)

    if r["estado"] == "ok":
        lines += [
            "## ✅ Procesado correctamente",
            "",
            "> No se requiere ninguna acción adicional.",
            "> Descarga el archivo corregido y súbelo al nuevo workspace.",
            "",
        ]
    else:
        lines += [
            "## ⚠️ Procesado con observaciones",
            "",
            "> El archivo fue corregido automáticamente, pero hay puntos que requieren",
            "> revisión manual **antes de subirlo al nuevo workspace**.",
            "> Revisa la sección de intervención humana más abajo.",
            "",
        ]

    # ── Cambios automáticos ───────────────────────────────────────
    c = r["cambios"]
    lines += [
        "---",
        "## ✅ Cambios aplicados automáticamente",
        "",
        f"| Cambio | Resultado |",
        f"|--------|-----------|",
        f"| `USE CATALOG regional` | {'✅ Insertado al inicio del notebook' if c['uc_insertado'] else f'ℹ️ {c["uc_msg"]}'} |",
        f"| Widget `catalog_source` | {'✅ Agregado + consultas detectadas' if c.get('p9_lineas_dinamicas') else ('✅ Agregado' if c['p9_insertado'] else 'No aplica')} |",
        f"| `hive_metastore.` eliminados | {c['hms_eliminados']} de {c['hms_count']} encontrados |",
        f"| Rutas `/mnt/` reemplazadas | {c['mnt_reemplazados']} de {c['mnt_count']} encontradas |",
        "",
    ]

    if c["cambios_hms"]:
        lines += [f"### Detalle — `hive_metastore.` eliminados ({len(c['cambios_hms'])})", ""]
        for ch in c["cambios_hms"]:
            lines += [
                f"**Celda {ch['celda']}, línea {ch['linea']}**",
                "```diff",
                f"- {ch['antes']}",
                f"+ {ch['despues']}",
                "```",
                "",
            ]

    if c["cambios_mnt"]:
        lines += [f"### Detalle — rutas `/mnt/` reemplazadas ({len(c['cambios_mnt'])})", ""]
        for cm in c["cambios_mnt"]:
            lines += [
                f"**Celda {cm['celda']}, línea {cm['linea']}**",
                "```diff",
                f"- {cm['antes']}",
                f"+ {cm['despues']}",
                "```",
                "",
            ]

    if c.get("p9_lineas_dinamicas"):
        n_ld = len(c["p9_lineas_dinamicas"])
        lines += [
            f"### ⚠️ Consultas con variable dinámica — requieren actualización manual ({n_ld})",
            "",
            "> El script agregó `catalog_source` automáticamente, pero estas líneas",
            "> construyen consultas usando la variable de base de datos dinámicamente.",
            "> Deben actualizarse manualmente para incluir el catálogo.",
            "",
            "**Cómo corregirlas:**",
            "```python",
            "# ANTES",
            "spark.sql(f\"SELECT * FROM {database}.tabla\")",
            "# DESPUÉS",
            "spark.sql(f\"SELECT * FROM {catalog}.{database}.tabla\")",
            "```",
            "",
            "**Líneas afectadas:**",
            "",
        ]
        for ld in c["p9_lineas_dinamicas"]:
            lines += [
                f"**Celda {ld['celda']}, línea {ld['linea']}:**",
                "```python",
                f"  {ld['codigo']}",
                "```",
                "",
            ]


    # ── Flags que requieren acción ────────────────────────────────
    flags_accion = {k: v for k, v in r["flags"].items() if FLAGS_INFO.get(k, {}).get("requiere_accion")}
    flags_info   = {k: v for k, v in r["flags"].items() if not FLAGS_INFO.get(k, {}).get("requiere_accion")}

    if flags_accion:
        lines += [
            "---",
            "## ⚠️ Requiere intervención humana",
            "",
            "> Estos puntos **no pudieron corregirse automáticamente**.",
            "> Deben resolverse manualmente antes de importar el notebook en el nuevo workspace.",
            "",
        ]
        for flag_id, ocurrencias in flags_accion.items():
            info = FLAGS_INFO[flag_id]
            lines += [
                f"### [{flag_id}] {info['titulo']}",
                "",
                info["instruccion"],
                "",
                f"**Líneas afectadas ({len(ocurrencias)}):**",
                "",
            ]
            for oc in ocurrencias:
                lines += [
                    f"- **Celda {oc['celda']}, línea {oc['linea']}:**",
                    f"  ```python",
                    f"  {oc['codigo']}",
                    f"  ```",
                    "",
                ]

    # ── Flags informativos ────────────────────────────────────────
    if flags_info:
        lines += [
            "---",
            "## ℹ️ Observaciones informativas",
            "",
            "> Los siguientes puntos fueron detectados pero **no requieren ninguna acción**.",
            "",
        ]
        for flag_id, ocurrencias in flags_info.items():
            info = FLAGS_INFO[flag_id]
            lines += [
                f"### [{flag_id}] {info['titulo']}",
                "",
                info["instruccion"],
                "",
                f"**Líneas detectadas ({len(ocurrencias)}):**",
                "",
            ]
            for oc in ocurrencias:
                lines += [
                    f"- **Celda {oc['celda']}, línea {oc['linea']}:** `{oc['codigo']}`",
                ]
            lines.append("")

    lines += ["---", f"*Generado por Rewrite UC — Marathon / Superdeporte · {fecha}*"]
    return "\n".join(lines)

# ═══════════════════════════════════════════════════════════════════
# INTERFAZ STREAMLIT
# ═══════════════════════════════════════════════════════════════════

def main():
    st.set_page_config(
        page_title="Rewrite UC — Marathon",
        page_icon="🔄",
        layout="centered",
    )

    st.title("🔄 Rewrite Unity Catalog")
    st.markdown(
        "Sube tus notebooks exportados desde Databricks y descarga las versiones "
        "corregidas para el nuevo workspace Unity Catalog."
    )
    st.markdown("---")

    # ── Instrucciones colapsables ──────────────────────────────────
    with st.expander("📋 ¿Cómo usar esta herramienta?"):
        st.markdown("""
**Paso 1 — Exportar el notebook desde Databricks (workspace anterior)**
1. Abre el notebook en Databricks
2. Ve a **File → Export → IPython Notebook (.ipynb)**
3. Guarda el archivo en tu computador

> ⚠️ Exporta como **.ipynb**, no como DBC Archive.

**Paso 2 — Subir y procesar aquí**
1. Sube el archivo `.ipynb` usando el botón de abajo
2. Haz clic en **Procesar notebooks**
3. Revisa el resultado de cada notebook

**Paso 3 — Descargar y subir al nuevo workspace**
1. Descarga el archivo corregido
2. Abre el nuevo workspace (marathon-nopro-v2)
3. Navega a tu carpeta personal
4. Importa el archivo `.ipynb` corregido
        """)

    st.markdown("---")

    # ── Zona de subida ─────────────────────────────────────────────
    archivos = st.file_uploader(
        "Sube uno o más notebooks (.ipynb)",
        type=["ipynb"],
        accept_multiple_files=True,
        help="Exporta desde Databricks: File → Export → IPython Notebook (.ipynb)",
    )

    if not archivos:
        st.info("👆 Sube al menos un archivo `.ipynb` para comenzar.")
        return

    st.markdown(f"**{len(archivos)} archivo(s) cargado(s):**")
    for f in archivos:
        st.markdown(f"- `{f.name}`")

    st.markdown("")

    if not st.button("🚀 Procesar notebooks", type="primary", use_container_width=True):
        return

    st.markdown("---")
    st.markdown("### Resultados")

    # ── Procesar ───────────────────────────────────────────────────
    resultados = []
    progress = st.progress(0, text="Procesando...")

    for i, archivo in enumerate(archivos):
        progress.progress((i + 1) / len(archivos), text=f"Procesando {archivo.name}...")
        resultado = procesar_notebook(archivo.name, archivo.read())
        resultados.append(resultado)
        render_resultado(resultado)
        st.markdown("")

    progress.empty()

    # ── Resumen ────────────────────────────────────────────────────
    st.markdown("---")
    n_ok       = sum(1 for r in resultados if r["estado"] == "ok")
    n_revision = sum(1 for r in resultados if r["estado"] == "revision")
    n_error    = sum(1 for r in resultados if r["estado"] == "error")

    col1, col2, col3 = st.columns(3)
    col1.metric("✅ Listos para subir", n_ok)
    col2.metric("⚠️ Requieren revisión", n_revision)
    col3.metric("❌ Con error", n_error)

    # ── Descarga ───────────────────────────────────────────────────
    procesados = [r for r in resultados if r["estado"] != "error"]
    if not procesados:
        return

    st.markdown("### Descargar archivos")

    zip_bytes = generar_zip(procesados)
    fecha = datetime.now().strftime("%Y%m%d_%H%M")
    st.download_button(
        label="⬇️ Descargar archivos (.zip)",
        data=zip_bytes,
        file_name=f"notebooks_UC_{fecha}.zip",
        mime="application/zip",
        use_container_width=True,
        type="primary",
    )
    st.caption(
        "El ZIP contiene tres carpetas:\n"
        "- **corregidos/** — notebooks con el mismo nombre original, listos para importar en el nuevo workspace\n"
        "- **originales/** — copias de respaldo con sufijo `(ORIG)`\n"
        "- **reportes/** — detalle de cambios y flags por notebook"
    )
    if n_revision > 0:
        st.warning(
            f"⚠️ {n_revision} notebook(s) requieren revisión manual antes de subirse al nuevo workspace. "
            "Revisa el detalle de cada uno arriba."
        )


if __name__ == "__main__":
    main()

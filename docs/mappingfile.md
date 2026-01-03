# Mapping Bestand Documentatie

Dit document beschrijft alle opties en mogelijkheden van het YAML mapping bestand voor ingest2duck.

## Overzicht

Het mapping bestand is de centrale configuratie voor ingest2duck. Het definieert:
- Welke bronnen (sources) moeten worden verwerkt
- Waar de data moet worden opgeslagen (destination)
- Hoe de data moet worden getransformeerd (collections)
- Welke outputs moeten worden aangemaakt (raw en normalized)

## Structuur

```yaml
version: 1
sources: [...]
destination: {...}
options: {...}
outputs: {...}
collections: {...}
xml_infer: {...}
```

## Root Niveau

### `version`
- **Type:** `integer`
- **Default:** `1`
- **Beschrijving:** Versie van het mapping formaat

```yaml
version: 1
```

---

## `sources` - Bron Definities

Een lijst van één of meer bronnen die moeten worden verwerkt. Elke bron definieert waar de data vandaan komt en hoe deze moet worden geïnterpreteerd.

### Algemene Source Eigenschappen

| Eigenschap | Type | Verplicht | Default | Beschrijving |
|------------|------|-----------|---------|--------------|
| `name` | `string` | **Ja** | - | Unieke naam van de bron (wordt gebruikt als prefix voor tabelnamen) |
| `url` | `string` | Nee* | - | URL naar de data (alternatief voor `file`). Ondersteunt template formatting (zie hieronder) |
| `file` | `string` | Nee* | - | Lokaal bestandspad (alternatief voor `url`) |
| `format` | `string` | Nee | `auto` | Formaat: `auto`, `xml`, `json`, `jsonl`, `csv`, `xlsx` |
| `timeout_s` | `integer` | Nee | `180` | Timeout in seconden voor URL downloads |
| `member` | `string` | Nee | - | Voor ZIP: specifiek bestand om uit te pakken |

*Minstens één van `url` of `file` is verplicht.

### URL Template Formatting

Je kunt dynamische waarden in URLs gebruiken met Python string format syntax. Plaatsholders worden automatisch vervangen bij het downloaden.

| Placeholder | Type | Beschrijving |
|------------|------|-------------|
| `{today}` | date | Huidige datum |
| `{yesterday}` | date | Datum van gisteren |
| `{tomorrow}` | date | Datum van morgen |
| `{now}` | datetime | Huidige datum en tijd |

### Datum/Tijd Format Strings

Gebruik standaard Python date/datetime format strings:
- `%Y` - 4-cijferig jaar (2026)
- `%m` - 2-cijferige maand (01-12)
- `%d` - 2-cijferige dag (01-31)
- `%H` - Uur (00-23)
- `%M` - Minuut (00-59)
- `%S` - Seconde (00-59)

Veelgebruikte format strings:
- `{today:%Y%m%d}` - YYYYMMDD (bijv. 20260103)
- `{today:%Y-%m-%d}` - YYYY-MM-DD (bijv. 2026-01-03)
- `{today:%Y}` - Jaar (bijv. 2026)
- `{today:%Y%m}` - Jaar+Maand (bijv. 202601)
- `{now:%Y%m%d%H%M%S}` - Datum+Tijd zonder scheidingstekens (bijv. 20260103143025)
- `{now:%Y-%m-%d %H:%M:%S}` - ISO-achtige formaat (bijv. 2026-01-03 14:30:25)

### Voorbeelden

#### ETIM export met datum van gisteren
```yaml
sources:
  - name: etim
    url: https://cdn.etim-international.com/exports/ETIMIXF3_1_{yesterday:%Y%m%d}.xml
```
Wordt geformatteerd naar: `https://cdn.etim-international.com/exports/ETIMIXF3_1_20260102.xml`

#### CBS export met jaartal
```yaml
sources:
  - name: cbs
    url: https://www.cbs.nl/exports/commoditycodes-{today:%Y}.xlsx
```
Wordt geformatteerd naar: `https://www.cbs.nl/exports/commoditycodes-2026.xlsx`

#### API export met datum parameter
```yaml
sources:
  - name: api_export
    url: https://api.example.com/export?date={today:%Y-%m-%d}&format=json
```
Wordt geformatteerd naar: `https://api.example.com/export?date=2026-01-03&format=json`

#### Tijdsgebonden export
```yaml
sources:
  - name: hourly_stats
    url: https://stats.example.com/hourly_{now:%Y%m%d%H%M%S}.csv
```
Wordt geformatteerd naar: `https://stats.example.com/hourly_20260103143025.csv`

> **Opmerking:** De checksum van de bron verandert elke keer als de geformatteerde URL verschilt. De bron wordt dus dagelijks (of vaker bij `{now}`) opnieuw gedownload, wat meestal de bedoeling is.

### CSV Specifieke Eigenschappen

| Eigenschap | Type | Default | Beschrijving |
|------------|------|---------|--------------|
| `delimiter` | `string` | `,` | Delimiter (bijv. `;`, `\t`) |
| `encoding` | `string` | `utf-8` | Tekstcodering |

### XLSX Specifieke Eigenschappen

| Eigenschap | Type | Default | Beschrijving |
|------------|------|---------|--------------|
| `sheet` | `string` | - | Specifiek tabblad naam |
| `use_first_sheet` | `boolean` | `false` | Gebruik het eerste tabblad |

### JSON/JSONL Specifieke Eigenschappen

| Eigenschap | Type | Default | Beschrijving |
|------------|------|---------|--------------|
| `records_path` | `string` | - | Dotted pad naar records (bijv. `data.items`) |

### Source-Level `collections`

Voor tabulaire formats (CSV/XLSX) en JSON kun je op source-niveau collecties definiëren die de globaal gedefinieerde `collections` overschrijven.

```yaml
sources:
  - name: cbs2025
    url: https://example.com/data.xlsx
    collections:
      commoditycodes:
        sheet: CN2025
        use_first_sheet: false
        write_disposition: replace
```

### Voorbeelden

#### CSV Bestand
```yaml
sources:
  - name: sales_data
    file: ./data/sales.csv
    format: csv
    delimiter: ";"
    encoding: "utf-8"
```

#### XLSX Bestand (meerdere tabbladen)
```yaml
sources:
  - name: financial_reports
    url: https://example.com/reports.xlsx
    collections:
      income:
        sheet: IncomeStatement
      balance:
        sheet: BalanceSheet
```

#### JSON Bestand
```yaml
sources:
  - name: api_data
    url: https://api.example.com/data.json
    records_path: results.items
```

#### ZIP Archief
```yaml
sources:
  - name: archive_data
    url: https://example.com/archive.zip
    member: data.xml
    format: xml
```

---

## `destination` - Bestemming Configuratie

Waar de opgeslagen data terecht moet komen.

### Algemene Eigenschappen

| Eigenschap | Type | Verplicht | Default | Beschrijving |
|------------|------|-----------|---------|--------------|
| `type` | `string` | Nee | `duckdb` | Type: `duckdb` of `ducklake` |
| `dataset` | `string` | **Ja** | - | Dataset naam (wordt gebruikt als schema naam) |

### DuckDB Specifiek

| Eigenschap | Type | Verplicht | Default | Beschrijving |
|------------|------|-----------|---------|--------------|
| `duckdb_file` | `string` | **Ja** (voor `type=duckdb`) | - | Pad naar DuckDB bestand |

```yaml
destination:
  type: duckdb
  dataset: my_dataset
  duckdb_file: ./data/my_database.duckdb
```

### DuckLake Specifiek

| Eigenschap | Type | Verplicht | Default | Beschrijving |
|------------|------|-----------|---------|--------------|
| `ducklake.ducklake_name` | `string` | Nee | `{dataset}` | DuckLake instantie naam |
| `ducklake.catalog` | `string` | Nee | - | Catalog connection string (bijv. `sqlite:///catalog.sqlitedb`) |
| `ducklake.storage` | `string` | Nee | - | Pad naar storage |

```yaml
destination:
  type: ducklake
  dataset: etim
  ducklake:
    ducklake_name: ducklake
    catalog: sqlite:///ducklake.sqlitedb
    storage: ./ducklake/
```

---

## `options` - Globale Opties

| Eigenschap | Type | Default | Beschrijving |
|------------|------|---------|--------------|
| `infer_if_missing` | `boolean` | `false` | Infer mappings automatisch als ze ontbreken |
 | `write_disposition` | `string` | `append` | Globale write disposition: `append`, `replace`, `merge`, of `skip` |

```yaml
options:
  infer_if_missing: true
  write_disposition: append
```

### `infer_if_missing`

Indien `true`, zal ingest2duck:
- XML: automatisch collections infereren van het XML bestand
- JSON: automatisch collecties infereren op basis van de JSON structuur
- CSV/XLSX: automatisch kolommen en headers detecteren

---

## `outputs` - Output Configuratie

Definieert welke outputs worden aangemaakt en hoe ze worden genoemd.

### `raw` - Raw Output

Alle data in één tabel met metadata, ongeformatteerd.

| Eigenschap | Type | Default | Beschrijving |
|------------|------|---------|--------------|
| `enabled` | `boolean` | `true` | Of raw tabel wordt aangemaakt |
| `table` | `string` | `raw_ingest` | Naam van de raw tabel |

De raw tabel bevat de volgende kolommen:
- `collection` - Naam van de collectie
- `path` - Pad in de bron (sheet naam, XML path, etc.)
- `raw_json` - De ruwe JSON data
- `__source_name` - Naam van de bron
- `__source_url` - URL van de bron
- `__source_type` - Type van de bron (`url` of `file`)
- `__ingest_timestamp` - Tijdstempel van ingest
- `__source_checksum` - SHA256 checksum van de bron
- `__source_size_bytes` - Grootte van de bron in bytes
- `__format` - Formaat van de data
- `__run_id` - Unieke run identificatie
- `__dataset` - Dataset naam

### `normalized` - Genormaliseerde Output

Per collectie een afzonderlijke tabel met semi-geflatteerde data.

| Eigenschap | Type | Default | Beschrijving |
|------------|------|---------|--------------|
| `enabled` | `boolean` | `true` | Of normalized tabellen worden aangemaakt |
| `tables` | `list` | `[]` | Lijst van tabelnamen (wordt automatisch bijgewerkt) |

```yaml
outputs:
  raw:
    enabled: true
    table: raw_ingest
  normalized:
    enabled: true
    tables:
      - cbs2025_commoditycodes
      - cbs2026_commoditycodes
```

---

## `collections` - Collectie Mappings

Globale collectie configuraties voor JSON, CSV, en XLSX. De tabelnaam moet beginnen met `{source_name}_`.

### Algemene Collectie Eigenschappen

| Eigenschap | Type | Default | Beschrijving |
|------------|------|---------|--------------|
| `enabled` | `boolean` | `true` | Of de collectie wordt verwerkt |
| `pk.prefer` | `list` | `["id", "ID", "code", "Code", "key", "Key"]` | Voorkeursvolgorde voor primaire sleutel |

### JSON Specifieke Eigenschappen

| Eigenschap | Type | Default | Beschrijving |
|------------|------|---------|--------------|
| `path` | `string` | `$` | JSONPath of dotted pad naar records |
| `write_disposition` | `string` | - | Collectie-specifieke write disposition |

### CSV/XLSX Specifieke Eigenschappen

| Eigenschap | Type | Default | Beschrijving |
|------------|------|---------|--------------|
| `sheet` | `string` | - | XLSX: specifiek tabblad |
| `use_first_sheet` | `boolean` | `false` | XLSX: gebruik eerste tabblad |
| `pk.prefer` | `list` | `[]` | Voorkeursvolgorde voor primaire sleutel |
| `write_disposition` | `string` | - | Collectie-specifieke write disposition |

### Voorbeelden

#### JSON Collectie
```yaml
collections:
  api_data_records:
    enabled: true
    path: $.data.items
    pk:
      prefer: ["id", "ID", "uuid"]
    write_disposition: replace
```

#### XLSX Collectie
```yaml
collections:
  cbs2025_commoditycodes:
    enabled: true
    sheet: CN2025
    use_first_sheet: false
    pk:
      prefer: ["Code", "code"]
    write_disposition: append
```

---

## `xml_infer` - XML Specifieke Configuratie

XML heeft een speciale structuur voor het definiëren van collecties.

```yaml
xml_infer:
  {source_name}:
    root: IXF
    collections:
      Units:
        enabled: true
        path: /IXF/Units/Unit
        pk:
          prefer: ["Code", "@id", "id", "@ID", "ID"]
        parent: null
        parent_fk: null
      Translations:
        enabled: true
        path: /IXF/Units/Unit/Translations/Translation
        pk:
          prefer: ["@language", "@id"]
        parent: Units
        parent_fk: _parent_pk
```

### XML Collectie Eigenschappen

| Eigenschap | Type | Default | Beschrijving |
|------------|------|---------|--------------|
| `enabled` | `boolean` | `true` | Of de collectie wordt verwerkt |
| `path` | `string` | - | XPath naar de entities (ondersteund: `/A/B`, `/A/B/C`, `/A/B/C/D/E`) |
| `pk.prefer` | `list` | - | Voorkeursvolgorde voor primaire sleutel (incl. `@` prefix voor attributen) |
| `parent` | `string` | `null` | Parent collectie naam voor hiërarchische relaties |
| `parent_fk` | `string` | `null` | Foreign key kolom naam (meestal `_parent_pk`) |

### XML PK Voorkeursvolgorde

Standaard wordt gezocht naar:
- Attributen: `@id`, `@ID`, `@code`, `@Code`, `@key`
- Elementen: `id`, `ID`, `code`, `Code`, `key`

Bij automatisch infereren worden velden met `changeCode`, `changeType`, `status`, `type`, `version`, of `flag` vermeden als PK.

### XML Hiërarchie

Voor geneste collecties (pad lengte 5):
- De parent collectie wordt bepaald op basis van de eerste 3 segmenten
- De child collectie heeft een `parent` en `parent_fk` verwijzing

---

## Primaire Sleutels (PK)

### Hoe PK Wordt Bepaald

1. **Voorkeurslijst (`pk.prefer`):** Het eerste veld uit de lijst dat een waarde heeft en uniek wordt gebruikt
2. **Stable Hash:** Als geen voorkeursveld werkt, wordt een SHA1 hash van alle data gebruikt

### Auto-Inferentie

Bij `infer_if_missing: true` worden PK's automatisch bepaald:
- XML: Op basis van attributen en elementen, geordend op uniciteit en null-rate
- JSON/CSV/XLSX: Op basis van kolomnamen (`id`, `ID`, `code`, `Code`, `key`, `Key`)

---

## Write Disposition

Definieert hoe data wordt geschreven naar tabellen.

| Waarde | Beschrijving |
|--------|-------------|
| `append` | Voeg nieuwe data toe aan bestaande tabel (default) |
| `replace` | Vervang de volledige tabel |
| `merge` | Dedupliceert en merged data op basis van `primary_key` |
| `skip` | Voorkomt dat data wordt geladen |

> **⚠️ Belangrijk:** Voor `write_disposition: merge` moet je een expliciete `pk.prefer` definiëren in je mapping om duplicaten te kunnen detecteren.

Kan worden ingesteld op:
- **Globaal niveau:** `options.write_disposition`
- **Collectie niveau:** `collections.{name}.write_disposition` of `source.collections.{name}.write_disposition`

---

## CLI Overrides

Mapping waarden kunnen worden overschreven via command-line argumenten:

| Argument | Overschrijft |
|----------|--------------|
| `--dataset` | `destination.dataset` |
| `--duckdb-file` | `destination.duckdb_file` |
| `--destination-type` | `destination.type` |
| `--ducklake-name` | `destination.ducklake.ducklake_name` |
| `--ducklake-catalog` | `destination.ducklake.catalog` |
| `--ducklake-storage` | `destination.ducklake.storage` |
| `--infer-if-missing` | `options.infer_if_missing` |
| `--write-disposition` | `options.write_disposition` |
| `--raw-table` | `outputs.raw.table` |

Voorbeeld:
```bash
python ingest2duck.py --mapping mapping.yml --dataset production --write-disposition replace
```

---

## Compleet Voorbeeld

```yaml
version: 1
sources:
  - name: cbs2025
    url: https://example.com/codes-2025.xlsx
    collections:
      commoditycodes:
        sheet: CN2025
        use_first_sheet: false

  - name: etim
    url: https://example.com/etim.xml

destination:
  type: duckdb
  dataset: my_dataset
  duckdb_file: ./data/my_database.duckdb

options:
  infer_if_missing: true
  write_disposition: append

outputs:
  raw:
    enabled: true
    table: raw_ingest
  normalized:
    enabled: true
    tables: []

collections:
  cbs2025_commoditycodes:
    enabled: true
    sheet: CN2025
    use_first_sheet: false
    pk:
      prefer: ["Code", "code"]

xml_infer:
  etim:
    root: IXF
    collections:
      Units:
        enabled: true
        path: /IXF/Units/Unit
        pk:
          prefer: ["Code", "@id", "id"]
        parent: null
        parent_fk: null
      Translations:
        enabled: true
        path: /IXF/Units/Unit/Translations/Translation
        pk:
          prefer: ["@language", "@id"]
        parent: Units
        parent_fk: _parent_pk
```

---

## Best Practices

1. **Gebruik `infer_if_missing: true`** voor eerste runs om automatisch mappings te genereren
2. **Specificeer PK's expliciet** voor betere query performance en data integriteit
3. **Gebruik `write_disposition: replace`** met zorg - dit verwijdert alle bestaande data
4. **Gebruik source-level `collections`** om specifieke sheets of paden per source te configureren
5. **Bekijk `outputs.normalized.tables`** na een run om alle aangemaakte tabellen te zien
6. **Test met kleine datasets** voordat je grote bestanden verwerkt

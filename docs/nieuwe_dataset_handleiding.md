# Handleiding: Nieuwe Dataset Toevoegen en Mappen

Deze handleiding legt stap voor stap uit hoe je een nieuwe URL instelt voor een dataset, de mapping aanpast naar jouw wensen, en vervolgens de workflow test met DuckDB voordat je overschakelt naar DuckLake.

---

## Inhoudsopgave

1. [Overzicht van het Proces](#overzicht-van-het-proces)
2. [Stap 1: Nieuwe Mapping File Aanmaken](#stap-1-nieuwe-mapping-file-aanmaken)
3. [Stap 2: Eerste Run met DuckDB](#stap-2-eerste-run-met-duckdb)
4. [Stap 3: Data Inspecteren met DuckDB](#stap-3-data-inspecteren-met-duckdb)
5. [Stap 4: Mapping Aanpassen](#stap-4-mapping-aanpassen)
6. [Stap 5: Overschakelen naar DuckLake](#stap-5-overschakelen-naar-ducklake)
7. [Voorbeelden per Data Type](#voorbeelden-per-data-type)
8. [Veelvoorkomende Problemen](#veelvoorkomende-problemen)

---

## Overzicht van het Proces

Het proces om een nieuwe dataset toe te voegen bestaat uit 5 hoofdstappen:

```
┌─────────────────────┐
│ 1. Mapping file     │
│    aanmaken         │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐     ┌──────────────┐
│ 2. Eerste run met   │────▶│ DuckDB DB    │
│    DuckDB (testen)  │     │ (lokaal)     │
└──────────┬──────────┘     └──────────────┘
           │
           ▼
┌─────────────────────┐
│ 3. Data inspecteren │
│    en mapping       │
│    aanpassen        │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ 4. Run met gewijzigde│
│    mapping          │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐     ┌──────────────┐
│ 5. Overschakeling    │────▶│ DuckLake     │
│    naar DuckLake     │     │ (productie)  │
└─────────────────────┘     └──────────────┘
```

**Waarom DuckDB eerst?**
- Snel en lokaal - geen externe database nodig
- Makkelijk te inspecteren met `duckdb` CLI
- Ideaal voor iteratief testen van mappings
- Gewijzigde mappings direct zichtbaar

---

## Stap 1: Nieuwe Mapping File Aanmaken

Gebruik de `--newmapping` optie om automatisch een template aan te maken:

```bash
python ingest2duck.py --newmapping mijn_dataset.yml
```

Hierdoor wordt een `mijn_dataset.yml` bestand aangemaakt met:
- Alle opties als commentaar (met uitleg)
- Automatische DuckDB verwijzing (`./mijn_dataset.duckdb`)
- Voorbeelden voor XLSX, CSV, JSON, en XML
- Naar DuckDB destination (klaar voor testen)

### Handmatige Aanpassing

Na het aanmaken van de template, pas de sources aan door de comment tekens (`#`) te verwijderen en de waarden in te vullen. Bijvoorbeeld:

```yaml
sources:
  - name: mijn_dataset
    url: https://example.com/data.xlsx
```

### Alternatief: Handmatig Aanmaken

Je kunt ook een mapping file handmatig aanmaken. Een minimale configuratie ziet er als volgt uit:

```yaml
version: 1

sources:
  - name: mijn_dataset
    url: https://example.com/data.xlsx

destination:
  type: duckdb
  dataset: mijn_dataset
  duckdb_file: ./mijn_dataset_test.duckdb

options:
  infer_if_missing: true

outputs:
  raw:
    enabled: true
    table: raw_ingest
  normalized:
    enabled: true
    tables: []
```

### Belangrijke onderdelen:

- **`sources.name`**: Unieke naam voor je bron (gebruik lowercase, underscores)
- **`sources.url`**: URL naar je data bestand
- **`destination.type`**: Start altijd met `duckdb` voor testen
- **`destination.dataset`**: Naam van het schema
- **`destination.duckdb_file`**: Lokaal DuckDB bestand
- **`options.infer_if_missing`**: Zet op `true` voor automatische mapping detectie bij eerste run
- **`outputs.normalized.tables`**: Laat leeg `[]` - wordt automatisch bijgewerkt

---

## Stap 2: Eerste Run met DuckDB

Voer de eerste run uit om de data te downloaden en automatisch mappings te detecteren:

```bash
python ingest2duck.py --mapping mijn_dataset.yml
```

**Wat er gebeurt:**
1. Het bestand wordt gedownload naar een tijdelijke locatie
2. Het formaat wordt automatisch gedetecteerd (XML, JSON, CSV, XLSX)
3. Mappings worden geïnferd op basis van de data structuur
4. De mapping file wordt bijgewerkt met de geïnferde instellingen
5. Data wordt geladen in de DuckDB database

### Mogelijke outputs:

**Bij XLSX/CSV:**
```
INFO - Inferred 1 tabular collections
INFO - Saved tabular mapping to configuration
```

**Bij JSON:**
```
INFO - Inferred 2 JSON collections
INFO - Saved JSON mapping to configuration
```

**Bij XML:**
```
INFO - Inferring XML mapping...
INFO - Saved XML mapping to configuration
```

Na succesvolle run zie je:
```
Done.
- run_id      : abc123...
- destination : duckdb
- duckdb-file : /pad/naar/mijn_dataset_test.duckdb
- dataset     : mijn_dataset
- mapping     : /pad/naar/mijn_dataset.yml
- sources     : mijn_dataset
```

---

## Stap 3: Data Inspecteren met DuckDB

Nu kun je de data inspecteren met de DuckDB CLI:

```bash
duckdb ./mijn_dataset_test.duckdb
```

### Overzicht van alle tabellen:

```sql
.show tables;
```

Typische tabellen:
- `raw_ingest` - Alle data in één tabel (niet genormaliseerd)
- `{source_name}_{collection}` - Genormaliseerde tabellen
- `sourcesmetadata` - Metadata over de sources

### Raw tabel inspecteren:

```sql
SELECT collection, COUNT(*) as row_count
FROM raw_ingest
GROUP BY collection;
```

### Genormaliseerde tabel inspecteren:

Voor een XLSX/CSV bron:
```sql
SELECT * FROM mijn_dataset_commoditycodes LIMIT 5;
```

Voor een JSON bron:
```sql
SELECT * FROM mijn_dataset_data LIMIT 5;
```

### Kolomnamen en types bekijken:

```sql
DESCRIBE mijn_dataset_commoditycodes;
```

### Schema van de raw tabel:

```sql
DESCRIBE raw_ingest;
```

### Data sample bekijken:

```sql
-- Bekijk de JSON structure van raw data
SELECT collection, raw_json
FROM raw_ingest
LIMIT 1;
```

### Exit DuckDB:

```sql
.exit
```

---

## Stap 4: Mapping Aanpassen

Nadat je de data hebt geïnspecteerd, kun je de mapping aanpassen aan jouw wensen. De mapping file is nu bijgewerkt met de automatisch gegenereerde instellingen.

### 4.1 Bekijk de huidige mapping

Open je mapping file:

```bash
cat mijn_dataset.yml
```

Je ziet nu automatisch gegenereerde secties zoals:
- `collections:` - voor JSON, CSV, XLSX
- `xml_infer:` - voor XML

### 4.2 Mapping Aanpassingen

#### A. Primaire Sleutel (PK) Aanpassen

De PK wordt automatisch gedetecteerd, maar je kunt dit overschrijven:

```yaml
collections:
  mijn_dataset_commoditycodes:
    enabled: true
    sheet: CN2025
    pk:
      prefer:
        - Code          # Prefer "Code" kolom als PK
        - code          # Fallback op lowercase
        - ID            # Fallback op ID
    write_disposition: append
```

**Waarom PK belangrijk is:**
- Unieke identificatie van rijen
- Nodig voor `write_disposition: merge`
- Beter query performance

#### B. Write Disposition Wijzigen

```yaml
collections:
  mijn_dataset_commoditycodes:
    write_disposition: replace  # Opties: append, replace, merge, skip
```

**Opties:**
- `append`: Voeg nieuwe data toe (default)
- `replace`: Vervang de volledige tabel
- `merge`: Update bestaande en voeg nieuwe toe (vereist PK)
- `skip`: Sla deze tabel over

#### C. Specifieke Tabblad Selecteren (XLSX)

```yaml
collections:
  mijn_dataset_financials:
    sheet: IncomeStatement  # Specifiek tabblad
    enabled: true
    pk:
      prefer: ["id"]

  mijn_dataset_balance:
    sheet: BalanceSheet      # Ander tabblad
    enabled: true
    pk:
      prefer: ["id"]
```

#### D. JSON Path Aanpassen

```yaml
collections:
  mijn_dataset_records:
    path: $.data.items        # Dotted pad naar records
    enabled: true
    pk:
      prefer: ["uuid"]
```

#### E. Collecties Uitschakelen

```yaml
collections:
  mijn_dataset_commoditycodes:
    enabled: false            # Deze collectie wordt overgeslagen
```

### 4.3 Test de gewijzigde mapping

```bash
# Voer opnieuw uit met gewijzigde mapping
python ingest2duck.py --mapping mijn_dataset.yml
```

Inspecteer opnieuw in DuckDB:

```bash
duckdb ./mijn_dataset_test.duckdb
```

```sql
-- Check of de data correct is geladen
SELECT * FROM mijn_dataset_commoditycodes LIMIT 5;
```

```sql
-- Check het aantal rijen
SELECT COUNT(*) FROM mijn_dataset_commoditycodes;
```

### 4.4 Herhaal indien nodig

Herhaal stap 3 en 4 totdat de mapping correct is en de data eruitziet zoals gewenst.

---

## Stap 5: Overschakelen naar DuckLake

Als je tevreden bent met de mapping in DuckDB, kun je overschakelen naar DuckLake.

### 5.1 Pas de destination aan

Wijzig `destination.type` naar `ducklake` en voeg DuckLake specifieke configuratie toe:

```yaml
destination:
  type: ducklake
  dataset: mijn_dataset
  ducklake:
    ducklake_name: ducklake
    catalog: sqlite:///ducklake.sqlitedb
    storage: ./ducklake/
    replace_strategy: truncate-and-insert
```

**DuckLake Replace Strategieën:**

| Strategie | Beschrijving | Gebruik voor |
|-----------|--------------|--------------|
| `truncate-and-insert` | Direct inserts, geen staging | **Aanbevolen** voor ingest2duck (sneller, kleinere datasets) |
| `insert-from-staging` | Via staging, atomic writes | Productie met strikte consistente eisen |
| `staging-optimized` | Geoptimaliseerd staging | Grote datasets (>1GB per run) |

> **Belangrijk:** Als je `write_disposition: merge` gebruikt, maakt DuckLake automatisch staging tables aan, ongeacht de `replace_strategy`.

### 5.2 Optioneel: Outputs aanpassen

Je kunt de raw tabel uitschakelen voor productie:

```yaml
outputs:
  raw:
    enabled: false        # Raw tabel niet opslaan (bespaart ruimte)
    table: raw_ingest
  normalized:
    enabled: true         # Alleen genormaliseerde tabellen
    tables:
      - mijn_dataset_commoditycodes
```

### 5.3 Eerste run met DuckLake

```bash
python ingest2duck.py --mapping mijn_dataset.yml
```

### 5.4 Verify DuckLake Data

Als DuckLake gebruikmaakt van een SQL catalog (bijv. sqlite), kun je inspecteren:

```bash
duckdb ducklake.sqlitedb
```

```sql
-- Toon alle schema's
SHOW SCHEMAS;

-- Toon tabellen in je dataset
SHOW TABLES FROM mijn_dataset;

-- Sample data
SELECT * FROM mijn_dataset.mijn_dataset_commoditycodes LIMIT 5;
```

### 5.5 Workflow voor Updates

Als de source URL verandert (bijv. dagelijkse updates met datum placeholder):

```yaml
sources:
  - name: mijn_dataset
    url: https://example.com/data_{today:%Y%m%d}.xlsx
```

Bij elke run wordt:
1. De checksum gecontroleerd (via E-tag of SHA256)
2. De mapping gecontroleerd
3. Alleen als iets gewijzigd is → data opnieuw geladen

Forceer herverwerking indien nodig:

```bash
python ingest2duck.py --mapping mijn_dataset.yml --force
```

---

## Voorbeelden per Data Type

### Voorbeeld 1: XLSX Bestand

**Initiële mapping:**
```yaml
version: 1
sources:
  - name: financials
    url: https://example.com/financials.xlsx
destination:
  type: duckdb
  dataset: financials
  duckdb_file: ./financials_test.duckdb
options:
  infer_if_missing: true
outputs:
  raw:
    enabled: true
    table: raw_ingest
  normalized:
    enabled: true
    tables: []
```

**Na automatische infer (bijgewerkt):**
```yaml
collections:
  financials_data:
    enabled: true
    sheet: Sheet1
    use_first_sheet: true
    pk:
      prefer: []
```

**Aangepaste mapping:**
```yaml
collections:
  financials_income:
    enabled: true
    sheet: IncomeStatement
    pk:
      prefer: ["id"]
    write_disposition: replace

  financials_balance:
    enabled: true
    sheet: BalanceSheet
    pk:
      prefer: ["id"]
    write_disposition: replace
```

### Voorbeeld 2: JSON Bestand

**Initiële mapping:**
```yaml
version: 1
sources:
  - name: api_data
    url: https://api.example.com/data.json
destination:
  type: duckdb
  dataset: api
  duckdb_file: ./api_test.duckdb
options:
  infer_if_missing: true
outputs:
  raw:
    enabled: true
    table: raw_ingest
  normalized:
    enabled: true
    tables: []
```

**Na automatische infer:**
```yaml
collections:
  api_data_data:
    enabled: true
    path: $
    pk:
      prefer: ["id", "ID", "code", "Code", "key", "Key"]
```

**Aangepaste mapping voor geneste JSON:**
```yaml
sources:
  - name: api_data
    url: https://api.example.com/data.json
    records_path: results.items  # Specificeer pad naar records

collections:
  api_data_records:
    enabled: true
    path: $.results.items
    pk:
      prefer: ["uuid"]
    write_disposition: merge
```

### Voorbeeld 3: CSV Bestand met Aangepaste Delimiter

```yaml
version: 1
sources:
  - name: sales
    url: https://example.com/sales.csv
    format: csv
    delimiter: ";"      # Nederlandse CSV delimiter
    encoding: utf-8
destination:
  type: duckdb
  dataset: sales
  duckdb_file: ./sales_test.duckdb
options:
  infer_if_missing: true
outputs:
  raw:
    enabled: true
  normalized:
    enabled: true
    tables: []

collections:
  sales_data:
    enabled: true
    pk:
      prefer: ["invoice_id"]
    write_disposition: append
```

### Voorbeeld 4: XML Bestand

**Initiële mapping:**
```yaml
version: 1
sources:
  - name: etim
    url: https://example.com/etim.xml
destination:
  type: duckdb
  dataset: etim
  duckdb_file: ./etim_test.duckdb
options:
  infer_if_missing: true
outputs:
  raw:
    enabled: true
  normalized:
    enabled: true
    tables: []
```

**Na automatische infer:**
```yaml
xml_infer:
  etim:
    root: IXF
    collections:
      etim_Units:
        enabled: true
        path: /IXF/Units/Unit
        pk:
          prefer: ["Code", "@id", "id", "@ID", "ID"]
        parent: null
        parent_fk: null
      # ... meer collecties
```

**Aangepaste mapping:**
```yaml
xml_infer:
  etim:
    root: IXF
    collections:
      etim_Units:
        enabled: true
        path: /IXF/Units/Unit
        pk:
          prefer: ["Code"]  # Alleen Code gebruiken als PK
        parent: null
        parent_fk: null
      etim_Features:
        enabled: false     # Deze collectie uitschakelen
        path: /IXF/Features/Feature
        pk:
          prefer: ["Code", "@id"]
```

---

## Veelvoorkomende Problemen

### Probleem 1: Mapping niet gevonden

**Foutmelding:**
```
ValueError: XML mapping for source 'mijn_dataset' missing
```

**Oplossing:**
Zet `infer_if_missing: true` in de options:

```yaml
options:
  infer_if_missing: true
```

Voer de run opnieuw uit om automatisch mappings te genereren.

### Probleem 2: Geen primaire sleutel gevonden

**Symptoom:**
De tabel heeft een `_pk` kolom met lange hash-waarden.

**Oplossing:**
Specificeer handmatig een PK:

```yaml
collections:
  mijn_dataset_data:
    pk:
      prefer: ["id", "code", "uuid"]
```

### Probleem 3: Foute kolomnaam of datatype

**Symptoom:**
Data klopt niet met verwachting.

**Oplossing:**
Inspecteer in DuckDB:

```sql
DESCRIBE mijn_dataset_data;
SELECT * FROM mijn_dataset_data LIMIT 5;
```

Pas indien nodig de mapping aan of filter in de raw tabel:

```sql
SELECT raw_json FROM raw_ingest
WHERE collection = 'mijn_dataset_data'
LIMIT 1;
```

### Probleem 4: Bestand niet gevonden bij tweede run

**Symptoom:**
`FileNotFoundError` bij URL met datum placeholder.

**Oplossing:**
Controleer of de URL template correct is:

```yaml
url: https://example.com/data_{today:%Y%m%d}.xlsx
```

Test de URL handmatig in een browser om te verifiëren dat het bestand beschikbaar is.

### Probleem 5: Dubbele data na meerdere runs

**Symptoom:**
Tabel groeit onterecht.

**Oplossing:**
Gebruik `write_disposition: replace` voor volledige refresh:

```yaml
collections:
  mijn_dataset_data:
    write_disposition: replace
```

Of gebruik `merge` met een correcte PK:

```yaml
collections:
  mijn_dataset_data:
    write_disposition: merge
    pk:
      prefer: ["uuid"]
```

### Probleem 6: Staging tabellen blijven bestaan in DuckLake

**Symptoom:**
Tabellen zoals `mijn_dataset_staging.mijn_dataset_data` blijven achter.

**Oorzaak:**
Normaal bij gebruik van `write_disposition: merge`.

**Oplossing:**
Deze tabellen worden automatisch opgeruimd na succesvolle merge. Als ze blijven bestaan, controleer de logs. Handmatig opschonen:

```sql
DROP TABLE IF EXISTS mijn_dataset_staging.mijn_dataset_data;
```

---

## CLI Overrides

Je kunt mapping waarden tijdelijk overschrijven zonder het bestand te wijzigen:

```bash
# DuckDB bestand wijzigen
python ingest2duck.py --mapping mijn_dataset.yml --duckdb-file ./test.duckdb

# Dataset naam wijzigen
python ingest2duck.py --mapping mijn_dataset.yml --dataset prod_dataset

# DuckLake parameters wijzigen
python ingest2duck.py --mapping mijn_dataset.yml --ducklake-catalog postgresql://user:pass@host/db

# Write disposition tijdelijk wijzigen
python ingest2duck.py --mapping mijn_dataset.yml --write-disposition replace

# Forceer herverwerking
python ingest2duck.py --mapping mijn_dataset.yml --force
```

---

## Tips en Best Practices

1. **Gebruik `--newmapping`** om snel een template aan te maken met documentatie
2. **Start altijd met DuckDB** voor iteratieve ontwikkeling
3. **Gebruik `infer_if_missing: true`** bij eerste runs
4. **Specificeer PK's expliciet** voor betere performance
5. **Test met kleine datasets** voordat je grote bestanden verwerkt
6. **Controleer de raw tabel** voor probleemoplossing
7. **Gebruik `--force`** alleen wanneer nodig (bypass checksum)
8. **Bewerk de mapping direct** voor snel feedback
9. **Zet `raw.enabled: false`** in productie om ruimte te besparen
10. **Gebruik datum placeholders** `{today}` voor dagelijkse updates
11. **Back-up je mapping bestand** na grote wijzigingen

---

## Samenvatting

| Stap | Actie | Doel |
|------|-------|------|
| 1 | Mapping template aanmaken met `--newmapping` | Snel starten met gedocumenteerd template |
| 2 | Source toevoegen en eerste run met DuckDB | Auto-infer mapping en laad data |
| 3 | Inspecteren met DuckDB | Begrijp data structuur |
| 4 | Mapping aanpassen | Pas PK, write disposition, etc. aan |
| 5 | Overschakelen naar DuckLake | Productie setup |

Met deze workflow kun je snel nieuwe datasets toevoegen en iteratief perfectioneren voordat je ze naar productie brengt in DuckLake.

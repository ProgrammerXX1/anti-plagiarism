# detect_norm_cpp

C++17 microservice for anti-fraud detection in academic documents. Detects symbol substitution (homoglyphs), anomalous spaces, and hidden text.

Port `8001` | REST JSON API | Swagger UI at `/docs`

---

## Detection Categories

The service performs 3 types of checks, always in this order:

### 1. change_word — Symbol Substitution (Homoglyphs)

Detects words containing characters from mixed Unicode scripts (Cyrillic + Latin). The dominant script of the word is determined by character count; foreign characters are flagged individually.

| What is detected | Example |
|---|---|
| Latin char in Cyrillic word | `информaтике` — Latin `a` (U+0061) among Cyrillic |
| Cyrillic char in Latin word | `Mоsсоw` — Cyrillic `о` (U+043E) among Latin |

**Languages:** Russian, English, Kazakh, Turkish.

**Known homoglyph pairs (16):**

| Cyrillic | Latin | Codepoints |
|----------|-------|------------|
| а, е, о, с, р, у, х | a, e, o, c, p, y, x | U+0430↔U+0061, ... |
| А, Е, О, С, Р, У, Х | A, E, O, C, P, Y, X | U+0410↔U+0041, ... |
| і, І (Ukrainian/Kazakh) | i, I | U+0456↔U+0069 |

**Implementation:** ICU `uscript_getScript()` for script classification. Single-char words are skipped. `limit` is always `1` (one foreign character per finding).

### 2. spaces — Anomalous Spaces

Detects 3 subcategories:

| Subcategory | What is detected | Examples |
|---|---|---|
| Multiple ASCII spaces | 2+ consecutive regular spaces | `слово····слово` (4 spaces) |
| Unicode spaces | NBSP, EM SPACE, EN SPACE, THIN SPACE, FIGURE SPACE, etc. | U+00A0, U+2003, U+2009 |
| Zero-width characters | ZWSP, ZWJ, ZWNJ, BOM, WORD JOINER, LRM, RLM | U+200B, U+200D, U+FEFF |

**Full list of detected Unicode spaces:** U+00A0, U+2000–U+200A, U+202F, U+205F, U+3000

**Full list of detected zero-width chars:** U+200B, U+200C, U+200D, U+200E, U+200F, U+2060, U+FEFF, U+034F

A single regular space is **not** a finding — only 2+ consecutive or special Unicode characters. `limit` = number of consecutive spaces (for multi-space), or `1` (for Unicode/zero-width).

### 3. hidden_symbols — Hidden Text

Detects text that is visually invisible to the reader. **Only works in formats with style information** (DOCX, PPTX, ODT).

| Check | Threshold | Description |
|---|---|---|
| Near-white text | RGB > (240, 240, 240) | All 3 color components exceed 240 |
| Text color ≈ background | Euclidean distance < 30 | `sqrt(dR² + dG² + dB²) < 30` |
| Micro-font | size < 2pt | Font size less than 2 points |

**Priority:** near-white → color≈background → micro-font. Each text run is flagged at most once (first matching condition wins). `limit` = text length in Unicode codepoints.

---

## Supported Formats

**Maximum file size: 50 MB**

| Format | Parser | Technology | change_word | spaces | hidden_symbols |
|--------|--------|------------|:-----------:|:------:|:--------------:|
| `.docx` | ZIP + XML | pugixml — `word/document.xml` text runs with color, font, background | + | + | + |
| `.doc` | ZIP + XML | Treated as DOCX (works for newer .doc files saved as OOXML) | + | + | ~ |
| `.pptx` | ZIP + XML | pugixml — `ppt/slides/slide*.xml`, font size and color from `<a:rPr>` | + | + | + |
| `.odt` | ZIP + XML | pugixml — `content.xml` + styles from `office:automatic-styles` (fo:color, fo:font-size, fo:background-color) | + | + | + |
| `.rtf` | Text (UTF-8) | Line-by-line parsing, no style information | + | + | — |
| `.txt` | Text (UTF-8) | Line-by-line parsing | + | + | — |
| `.pdf` | poppler-cpp | Text layer extraction. No OCR — scanned pages are automatically skipped. No style information | + | ~ | — |

`+` full support · `~` partial (PDF spaces normalized by poppler; .doc limited compatibility) · `—` no style data available

### Format-specific notes

- **DOCX**: Only `word/document.xml` is parsed. Headers, footers, text boxes, and comments are not analyzed.
- **PPTX**: All slides `ppt/slides/slide*.xml` are parsed. Speaker notes and master slides are not analyzed.
- **ODT**: Styles from `office:automatic-styles` are applied. Linked styles from `styles.xml` are not resolved.
- **PDF**: poppler-cpp extracts the embedded text layer. Scanned pages (images without text) return empty — no OCR is attempted. Poppler may normalize multiple spaces based on character coordinates.
- **.doc**: Old binary Word format (pre-2007) is not a ZIP archive and cannot be parsed by the DOCX parser. Returns empty document.

---

## API

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Service status, version, engine |
| `GET` | `/formats` | List of supported file extensions |
| `POST` | `/detect` | Full detection (all 3 categories). Query params: `check_symbols`, `check_spaces`, `check_hidden` (boolean) |
| `POST` | `/detect/symbols` | Only change_word detection |
| `POST` | `/detect/spaces` | Only spaces detection |
| `POST` | `/detect/hidden` | Only hidden_symbols detection |
| `GET` | `/tests/list` | List test files |
| `POST` | `/tests/run/{file}` | Run detection on a test file |
| `GET` | `/tests/file/{file}` | Download a test file |
| `GET` | `/docs` | Swagger UI |
| `GET` | `/redoc` | ReDoc UI |
| `GET` | `/openapi.json` | OpenAPI 3.1 specification |

### Request

```bash
curl -X POST http://localhost:8001/detect \
  -F "file=@document.docx"
```

With options:
```bash
curl -X POST "http://localhost:8001/detect?check_symbols=true&check_spaces=true&check_hidden=false" \
  -F "file=@document.docx"
```

### Response — DetectionReport

```json
{
  "filename": "document.docx",
  "text": "Full extracted text of the document...\nSecond paragraph...",
  "total_findings": 3,
  "findings": [
    {
      "word": "написал",
      "offset": 17,
      "limit": 1,
      "fraud_type": "change_word"
    },
    {
      "word": "    ",
      "offset": 10,
      "limit": 4,
      "fraud_type": "spaces"
    },
    {
      "word": "Hidden white text",
      "offset": 27,
      "limit": 17,
      "fraud_type": "hidden_symbols"
    }
  ],
  "elapsed_sec": 0.00038,
  "errors": []
}
```

### Response fields

| Field | Type | Description |
|-------|------|-------------|
| `filename` | string | Uploaded file name |
| `text` | string | Full extracted document text. Paragraphs separated by `\n`. Scans/images are skipped |
| `total_findings` | integer | Total number of findings |
| `findings` | array | Array of Finding objects |
| `elapsed_sec` | number | Processing time in seconds |
| `errors` | array | Parsing errors (if any) |

### Finding fields

| Field | Type | Description |
|-------|------|-------------|
| `word` | string | Word or text containing the violation |
| `offset` | integer | Byte position from start of text run (UTF-8: Cyrillic = 2 bytes/char) |
| `limit` | integer | `change_word` → always 1; `spaces` → number of spaces; `hidden_symbols` → text length in codepoints |
| `fraud_type` | string | `change_word` \| `spaces` \| `hidden_symbols` |

---

## Architecture

```
src/
  main.cpp                   — HTTP server, routing, OpenAPI spec
  models.h / models.cpp      — Data structures, JSON serialization
  detectors/
    symbol_substitution.cpp  — change_word (ICU script classification)
    spacing_detector.cpp     — spaces (Unicode space/zero-width detection)
    hidden_text_detector.cpp — hidden_symbols (color/font analysis)
  parsers/
    txt_parser.cpp           — TXT, RTF (UTF-8 line splitting)
    docx_parser.cpp          — DOCX, DOC (ZIP + word/document.xml)
    pptx_parser.cpp          — PPTX (ZIP + ppt/slides/slide*.xml)
    odt_parser.cpp           — ODT (ZIP + content.xml + styles)
    pdf_parser.cpp           — PDF (poppler-cpp text layer)
    zip_xml_util.cpp         — ZIP extraction, shared by DOCX/PPTX/ODT
```

### Thread Safety

All components are fully thread-safe:
- Detectors use `static const` arrays and `constexpr` thresholds. Each call returns a new vector on the stack.
- Parsers create local `pugi::xml_document` and `zip_t*` handles per call. No shared state.
- ICU `uscript_getScript()` is thread-safe for read operations.
- Server globals (`VERSION`, `SUPPORTED_EXTS`, `tests_dir`, `static_dir`) are read-only after initialization.

### Security

- **Path traversal protection**: `is_safe_filename()` blocks `..`, `/`, `\`, hidden files (`.` prefix)
- **Zip bomb protection**: extracted entry size capped at 500 MB; `zip_stat_init()` + `zip_fread()` return value validated
- **PDF overflow protection**: file size checked against `INT_MAX` before passing to poppler
- **Payload limit**: 50 MB (`set_payload_max_length`)
- **CORS**: `Access-Control-Allow-Origin: *`
- **Global exception handler**: catches unhandled exceptions, returns JSON 500

---

## Configuration

| Parameter | Value | Location |
|-----------|-------|----------|
| Port | 8001 | `main.cpp:45`, `Dockerfile:39` |
| Max file size | 50 MB | `main.cpp:238` |
| Thread pool | 2 workers | `main.cpp:246` |
| Max queued requests | 32 | `main.cpp:246` |
| Read timeout | 30 sec | `main.cpp:240` |
| Write timeout | 30 sec | `main.cpp:241` |
| Keep-alive interval | 5 sec | `main.cpp:242` |
| Docker CPUs | 2 | `docker-compose.yml:10` |
| Docker memory | 2 GB | `docker-compose.yml:12` |
| Restart policy | unless-stopped | `docker-compose.yml:7` |

---

## Build & Deploy

### Dependencies

| Library | Version | Purpose |
|---------|---------|---------|
| [nlohmann/json](https://github.com/nlohmann/json) | 3.11.3 | JSON serialization |
| [cpp-httplib](https://github.com/yhirose/cpp-httplib) | 0.15.3 | HTTP server |
| [pugixml](https://github.com/zeux/pugixml) | 1.14 | XML parsing (DOCX, PPTX, ODT) |
| ICU | system | Unicode script classification |
| libzip | system | ZIP archive extraction |
| poppler-cpp | system | PDF text layer extraction |

nlohmann/json, cpp-httplib, and pugixml are fetched automatically via CMake `FetchContent`.

### Docker (recommended)

```bash
docker compose build
docker compose up -d
```

Service available at `http://localhost:8001`

### Local build

```bash
# Ubuntu 24.04
sudo apt install g++ cmake make pkg-config libicu-dev libzip-dev libpoppler-cpp-dev

mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)

# Run
./detect_norm_cpp 8001
```

---

## Test Files

| File | Description | Expected findings |
|------|-------------|-------------------|
| `test_hard.txt` | All homoglyph + space combinations | 84 (24 cw + 60 sp) |
| `test_hard_symbols.docx` | Dense homoglyph substitutions | 34 (34 cw) |
| `test_hard_spaces.docx` | All types of Unicode spaces | 103 (103 sp) |
| `test_hard_hidden.docx` | White text, micro-font, color matching | 13 (13 hs) |
| `test_hard_combined.docx` | All 3 categories combined | 23 (7 cw + 13 sp + 3 hs) |

Run all tests:
```bash
# Single file
curl -X POST http://localhost:8001/tests/run/test_hard_combined.docx

# All files via API
for f in $(curl -s http://localhost:8001/tests/list | jq -r '.files[]'); do
  echo "$f: $(curl -s -X POST http://localhost:8001/tests/run/$f | jq .total_findings) findings"
done
```

---

## Performance

| File | Size | Time |
|------|------|------|
| TXT 62 KB (500 paragraphs) | 62 KB | ~5 ms |
| DOCX test files | 5-15 KB | ~0.3 ms |
| Large DOCX | 1 MB | ~10 ms |

Engine: C++17 with `-O3 -march=native`, ICU for Unicode classification. All detectors work in-memory after file upload.

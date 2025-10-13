# Copilot Instructions for etl_web_portal

## Project Overview
This is a Flask-based ETL web portal for uploading, validating, and importing Amazon B2B/B2C and payout files into a PostgreSQL database. The main entry point is `app.py`, which orchestrates file uploads, data normalization, error handling, and database import.

## Architecture & Data Flow
- **Web UI**: HTML templates in `templates/` for login, upload, and status pages.
- **File Uploads**: Handled via `/upload` route in `app.py`. Uploaded files are saved to `uploads/`.
- **Data Processing**: Uploaded files are processed using pandas, with custom normalization and validation logic for different table types (see `optimize_dataframe`, `normalize_column_names`, etc.).
- **Database Import**: Data is imported into PostgreSQL using the `COPY` command for efficiency. Table schemas are defined in `TABLE_COLUMNS`.
- **Error Handling**: Custom error handlers for file size, validation, and database errors.

## Key Patterns & Conventions
- **File Validation**: Only `.csv`, `.xlsx`, `.xls` files are accepted. Use `secure_file_upload` and `allowed_file` for validation.
- **Column Normalization**: All columns are lowercased, stripped, and mapped to canonical names using fuzzy matching and explicit mappings (see payout logic).
- **Numeric & Date Handling**: Numeric columns are cleaned of commas/currency symbols; date columns are parsed with multiple format fallbacks.
- **Temporary Files**: Uploaded files are deleted after processing to avoid clutter.
- **Session Management**: Simple session-based login for admin access.

## Developer Workflows
- **Run Locally**: Start with `python app.py` (ensure Flask and dependencies in `requirements.txt` are installed).
- **Database Setup**: PostgreSQL must be running locally with a `spigen` schema and tables matching `TABLE_COLUMNS`.
- **Debugging**: Use print statements in `app.py` for tracing data flow and errors. Flask debug mode is enabled by default.
- **Error Recovery**: Check flash messages and console output for validation or database errors.

## Integration Points
- **External Dependencies**: pandas, Flask, psycopg2, werkzeug, and PostgreSQL.
- **Templates**: HTML files in `templates/` are tightly coupled to Flask routes.
- **Uploads**: All uploaded files are processed and then deleted from `uploads/`.

## Examples
- To add a new table type, update `TABLE_COLUMNS` and normalization logic in `app.py`.
- For custom validation, extend `validate_dataframe` or add new error handlers.

## Key Files
- `app.py`: Main application logic, routes, data processing, and DB import.
- `requirements.txt`: Python dependencies.
- `templates/`: HTML templates for UI.
- `uploads/`: Temporary file storage.

---
**Feedback Requested:**
If any section is unclear or missing details (e.g., build/test workflow, error handling, or integration specifics), please specify so instructions can be improved for future AI agents.

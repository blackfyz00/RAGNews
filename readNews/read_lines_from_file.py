import os

def read_lines_from_file(file_path: str) -> list[str]:
    """Читает строки из файла, удаляя пустые строки и пробелы."""
    if not os.path.exists(file_path):
        print(f"⚠️ Предупреждение: Файл {file_path} не найден.")
        return []
    with open(file_path, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    return lines

import pdfplumber
import logging
from io import BytesIO

logger = logging.getLogger(__name__)


def extract_text_from_stream(file_stream, file_path):
    """
    Extracts text from PDF or plain text files.

    Args:
        file_stream (bytes): The file content as bytes.
        file_path (str): Original file name (to determine type).

    Returns:
        str: Extracted text content.
    """
    logger.info(f"[DEBUG] Extracting text from file: {file_path}")

    # Ensure stream is a seekable file-like object
    if isinstance(file_stream, bytes):
        file_stream = BytesIO(file_stream)

    file_stream.seek(0)

    if file_path.lower().endswith('.pdf'):
        try:
            with pdfplumber.open(file_stream) as pdf:
                text = "\n".join([page.extract_text() for page in pdf.pages if page.extract_text()])
                logger.info(f"[INFO] Extracted {len(text)} characters from PDF {file_path}")
                return text
        except Exception as e:
            logger.warning(f"[WARN] Failed to extract text from PDF {file_path}: {e}")
            return ""

    try:
        text = file_stream.read().decode('utf-8', errors='ignore')
        logger.info(f"[INFO] Extracted {len(text)} characters from text file {file_path}")
        return text
    except Exception as e:
        logger.warning(f"[WARN] Failed to decode file {file_path}: {e}")
        return ""

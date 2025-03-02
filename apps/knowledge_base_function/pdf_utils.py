# pdf_utils.py
import fitz  # PyMuPDF

def extract_text_from_pdf(file_path):
    """Extract text from a PDF file."""
    doc = fitz.open(file_path)  # Open the PDF file
    text = ""
    for page in doc:
        text += page.get_text()  # Extract text from each page
    return text

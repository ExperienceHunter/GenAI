import subprocess

from flask import Flask, request, jsonify, render_template, session
import json
import traceback
import re
import os
from minio import Minio
from werkzeug.utils import secure_filename
from dotenv import load_dotenv  # Import dotenv to load environment variables
import logging
from apps.knowledge_base_function.process_documents import process_documents

# Initialize Flask app
from apps.models_function.deepseek_model import DeepseekModel
from apps.models_function.fine_tuned_tiny_llama_model import FineTunedTinyLlamaModel
from apps.models_function.llama_3_model import Llama3Model
from apps.models_function.tiny_llama_model import TinyLlamaModel

app = Flask(__name__)
app.secret_key = 'your_secret_key'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # Limit to 16MB

# Load environment variables from the .env file
load_dotenv()

# MinIO client configuration using environment variables
minio_host = os.getenv("MINIO_HOST")
minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")
document_bucket = os.getenv("DOCUMENT_BUCKET")  # MinIO bucket for documents

# Initialize MinIO client with values from the .env file
minio_client = Minio(
    minio_host,
    access_key=minio_access_key,
    secret_key=minio_secret_key,
    secure=False
)

# Ensure the document bucket exists
if not minio_client.bucket_exists(document_bucket):
    minio_client.make_bucket(document_bucket)

# Model dictionary for faster lookups (Example models)
models = {
    "deepseek": DeepseekModel(),
    "tiny_llama": TinyLlamaModel(),
    "fine_tuned_tiny_llama": FineTunedTinyLlamaModel(),
    "llama_3": Llama3Model(),
}

@app.route("/")
def main_page():
    return render_template("main_page.html")  # Ensure this template exists

@app.route("/chatbot")
def chatbot_page():
    return render_template("chatbot_page.html", models=list(models.keys()))

@app.route('/knowledge-base')
def knowledge_base():
    return render_template('knowledge_base_page.html')

@app.route("/choose_model", methods=["POST"])
def choose_model():
    model_choice = request.form.get("model_choice")

    if model_choice not in models:
        return jsonify({"error": "Invalid model choice!"}), 400

    session["selected_model"] = model_choice
    session["conversation_history"] = json.dumps([])

    return jsonify({"status": "success", "message": f"Model {model_choice} is selected!"})

@app.route("/ask_question", methods=["POST"])
def ask_question():
    model_name = session.get("selected_model")
    if not model_name:
        return jsonify({"error": "Please select a model first!"}), 400

    user_input = request.form.get("question")
    if not user_input:
        return jsonify({"error": "No question provided!"}), 400

    model = models.get(model_name)
    if not model:
        return jsonify({"error": "Invalid model selected!"}), 400

    conversation_history = json.loads(session.get("conversation_history", "[]"))

    try:
        answer = model.ask_question(user_input, conversation_history)  # Adjust this based on your model API
        formatted_answer = format_response(answer)
    except Exception as e:
        app.logger.error(f"Error while processing question: {traceback.format_exc()}")
        return jsonify({"error": f"Error while processing the question: {str(e)}"}), 500

    conversation_history.append(f"Q: {user_input}")
    conversation_history.append(f"A: {formatted_answer}")
    session["conversation_history"] = json.dumps(conversation_history)

    return jsonify({"answer": formatted_answer})

@app.route("/clear_chat", methods=["POST"])
def clear_chat():
    session["conversation_history"] = json.dumps([])
    return jsonify({"status": "success"})


@app.route("/upload_file", methods=["POST"])
def upload_file():
    try:
        # Check if file is in the request
        file = request.files.get('file')
        if not file:
            app.logger.error("No file received in the request!")
            return jsonify({"error": "No file uploaded!"}), 400

        # Read first 100 bytes for debugging
        file_content = file.read(100)
        app.logger.info(f"File content (first 100 bytes): {file_content}")

        # Reset the file stream position to the beginning
        file.seek(0)

        # Save the file temporarily
        temp_file_path = os.path.join('/tmp', file.filename)
        file.save(temp_file_path)

        # Get file size manually
        file_size = os.path.getsize(temp_file_path)
        app.logger.info(f"Manually calculated file size: {file_size} bytes")

        # Check if file size is 0
        if file_size == 0:
            app.logger.error(f"Received file with 0 bytes: {file.filename}")
            return jsonify({"error": "File size is 0!"}), 400

        # Upload to MinIO using fput_object
        minio_client.fput_object(
            document_bucket,  # Target bucket
            file.filename,  # File name in MinIO
            temp_file_path  # Path to the temporary saved file
        )

        # Clean up temporary file
        os.remove(temp_file_path)

        return jsonify({"message": "File uploaded successfully!"})

    except Exception as e:
        app.logger.error(f"Error while uploading the file: {str(e)}")
        return jsonify({"error": f"Error while uploading the file: {str(e)}"}), 500


@app.route('/vectorize', methods=['POST'])
def vectorize():
    try:
        app.logger.info("Vectorization request received.")

        # Call the process_documents function directly
        process_documents()

        app.logger.info("Vectorization completed successfully.")
        return jsonify({"success": True, "message": "Vectorization completed."})

    except Exception as e:
        app.logger.error("Error during vectorization: %s", str(e))
        return jsonify({"success": False, "error": str(e)})

def format_response(response):
    # If the response contains a numbered list (e.g., "1. Item"), convert it to an ordered list.
    if re.match(r'^\d+\.', response):
        lines = response.split('\n')
        list_items = ''.join(f'<li>{line.strip()}</li>' for line in lines if line.strip())
        return f'<ol>{list_items}</ol>'

    # If the response contains multiple lines, format it into paragraphs.
    elif '\n' in response:
        return ''.join(f'<p>{line.strip()}</p>' for line in response.split('\n') if line.strip())

    return response

if __name__ == "__main__":
    # Enable logging for debugging
    logging.basicConfig(level=logging.DEBUG)
    app.run(debug=True)

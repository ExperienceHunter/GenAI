from flask import Flask, request, jsonify, render_template, session
import json
import textwrap
import traceback
import re
from apps.models_function.deepseek_model import DeepseekModel
from apps.models_function.tiny_llama_model import TinyLlamaModel
from apps.models_function.fine_tuned_tiny_llama_model import FineTunedTinyLlamaModel
from apps.models_function.llama_3_model import Llama3Model

# Initialize Flask apps
app = Flask(__name__)
app.secret_key = 'your_secret_key'

# Model dictionary for faster lookups
models = {
    "deepseek": DeepseekModel(),
    "tiny_llama": TinyLlamaModel(),
    "fine_tuned_tiny_llama": FineTunedTinyLlamaModel(),
    "llama_3": Llama3Model(),
}


@app.route("/")
def main_page():
    # This will render the main page with a button to navigate to the chatbot page
    return render_template("main_page.html")  # Make sure to create this HTML page


@app.route("/chatbot")
def chatbot_page():
    # This will render the chatbot page
    return render_template("chatbot_page.html", models=list(models.keys()))


@app.route("/choose_model", methods=["POST"])
def choose_model():
    model_choice = request.form.get("model_choice")

    if model_choice not in models:
        return jsonify({"error": "Invalid model choice!"}), 400

    # Store the selected model name in session instead of using a global variable
    session["selected_model"] = model_choice
    session["conversation_history"] = json.dumps([])  # Store history as JSON string

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

    # Load conversation history correctly
    conversation_history = json.loads(session.get("conversation_history", "[]"))

    try:
        # Ensure question is not duplicated before calling the model
        answer = model.ask_question(user_input, conversation_history)
        formatted_answer = format_response(answer)  # Format the answer before sending
    except Exception as e:
        app.logger.error(f"Error while processing question: {traceback.format_exc()}")
        return jsonify({"error": f"Error while processing the question: {str(e)}"}), 500

    # Append only new question and answer (avoiding duplication)
    conversation_history.append(f"Q: {user_input}")
    conversation_history.append(f"A: {formatted_answer}")
    session["conversation_history"] = json.dumps(conversation_history)

    app.logger.info(f"Updated Conversation History: {conversation_history}")

    return jsonify({"answer": formatted_answer})


@app.route("/clear_chat", methods=["POST"])
def clear_chat():
    session["conversation_history"] = json.dumps([])  # Reset to an empty list
    app.logger.info("Chat history cleared successfully.")
    return jsonify({"status": "success"})


def format_response(response):
    # If the response contains a numbered list (e.g., "1. Item"), convert it to an ordered list.
    if re.match(r'^\d+\.', response):
        lines = response.split('\n')
        list_items = ''.join(f'<li>{line.strip()}</li>' for line in lines if line.strip())
        return f'<ol>{list_items}</ol>'

    # If the response contains multiple lines, format it into paragraphs.
    elif '\n' in response:
        return ''.join(f'<p>{line.strip()}</p>' for line in response.split('\n') if line.strip())

    # Otherwise, just return the response as it is.
    return response


if __name__ == "__main__":
    app.run(debug=True)

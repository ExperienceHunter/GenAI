from celery import shared_task
from app import models  # Import the models_function dictionary


@shared_task
def model_task(user_input, conversation_history, model_name):
    # Use model_name to fetch the appropriate model from the models_function dictionary
    model = models.get(model_name)

    if model is None:
        return "No model selected or invalid model name"

    try:
        answer = model.ask_question(user_input, conversation_history)
    except Exception as e:
        return f"Error while processing the question: {str(e)}"

    return answer

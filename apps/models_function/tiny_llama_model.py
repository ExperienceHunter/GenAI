import os
from dotenv import load_dotenv
from transformers import AutoTokenizer, AutoModelForCausalLM
import torch

# Load environment variables from .env file
load_dotenv()

class TinyLlamaModel:
    def __init__(self):
        # Get model path from environment variable
        self.local_model_path = os.getenv("TINYLLAMA_MODEL_PATH")

        if not self.local_model_path:
            raise ValueError("TINYLLAMA_MODEL_PATH environment variable is not set.")

        self.tokenizer = AutoTokenizer.from_pretrained(self.local_model_path)
        self.model = AutoModelForCausalLM.from_pretrained(
            self.local_model_path, torch_dtype=torch.float32, trust_remote_code=True
        )

        if not hasattr(self.model.config, "pad_token_id") or self.model.config.pad_token_id is None:
            self.model.config.pad_token_id = self.model.config.eos_token_id

        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model = self.model.to(self.device)

        # Initialize conversation history as an instance variable
        self.conversation_history = []

    def ask_question(self, question, conversation_history):
        MAX_TOKENS = 2048

        generation_params = {
            "max_length": 150,
            "num_beams": 1,
            "temperature": 0.7,
            "top_k": 50,
            "top_p": 0.9,
            "no_repeat_ngram_size": 2,
            "do_sample": True
        }

        conversation_history.append(f"Q: {question}")
        prompt = "\n".join(conversation_history[-1:]) + "\nA:"

        tokens = self.tokenizer.encode(prompt)
        token_length = len(tokens)

        while token_length > MAX_TOKENS:
            conversation_history.pop(0)
            prompt = "\n".join(conversation_history[-1:]) + "\nA:"
            tokens = self.tokenizer.encode(prompt)
            token_length = len(tokens)

        inputs = self.tokenizer(prompt, return_tensors="pt", padding=True, truncation=True).to(self.device)

        try:
            outputs = self.model.generate(
                inputs['input_ids'],
                attention_mask=inputs.get('attention_mask'),
                max_length=generation_params["max_length"],
                num_beams=generation_params["num_beams"],
                temperature=generation_params["temperature"],
                top_k=generation_params["top_k"],
                top_p=generation_params["top_p"],
                no_repeat_ngram_size=generation_params["no_repeat_ngram_size"],
                do_sample=generation_params["do_sample"],
                pad_token_id=self.model.config.pad_token_id
            )
        except Exception as e:
            print(f"Error during model inference: {e}")
            return

        generated_answer = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
        start = generated_answer.find("A:") + len("A:")
        end = generated_answer.find("Q:", start)

        if end == -1:
            answer = generated_answer[start:].strip()
        else:
            answer = generated_answer[start:end].strip()

        conversation_history.append(f"A: {answer}")

        print("\n".join(conversation_history))

        return answer

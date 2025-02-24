from transformers import AutoTokenizer, AutoModelForCausalLM
import torch


class DeepseekModel:
    def __init__(self):
        self.local_model_path = "/Users/zakwanzahid/PycharmProjects/GenAI/models/deepseek/DeepSeek-R1-Distill-Qwen-1.5B"
        self.tokenizer = AutoTokenizer.from_pretrained(self.local_model_path)
        self.model = AutoModelForCausalLM.from_pretrained(self.local_model_path, torch_dtype=torch.float32,
                                                          trust_remote_code=True)

        if not hasattr(self.model.config, "pad_token_id") or self.model.config.pad_token_id is None:
            self.model.config.pad_token_id = self.model.config.eos_token_id

        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model = self.model.to(self.device)

        # Initialize conversation history as an instance variable
        self.conversation_history = []

    def ask_question(self, question, conversation_history):
        MAX_TOKENS = 2048

        generation_params = {
            "max_new_tokens": 1500,
            "num_beams": 1,
            "temperature": 0.7,
            "top_k": 50,
            "top_p": 0.9,
            "no_repeat_ngram_size": 2,
            "do_sample": True
        }

        # Ensure the latest conversation is included, maintaining history
        prompt = "\n".join(conversation_history[-5:]) + f"\nQ: {question}\nA:"

        # Ensure token limit isn't exceeded
        tokens = self.tokenizer.encode(prompt)
        while len(tokens) > MAX_TOKENS:
            conversation_history.pop(0)  # Remove oldest entry
            prompt = "\n".join(conversation_history[-5:]) + f"\nQ: {question}\nA:"
            tokens = self.tokenizer.encode(prompt)

        inputs = self.tokenizer(prompt, return_tensors="pt", padding=True, truncation=True).to(self.device)

        try:
            outputs = self.model.generate(
                inputs["input_ids"],
                attention_mask=inputs.get("attention_mask"),
                **generation_params,
                pad_token_id=self.model.config.pad_token_id
            )
        except Exception as e:
            print(f"Error during model inference: {e}")
            return ""

        generated_answer = self.tokenizer.decode(outputs[0], skip_special_tokens=True)

        # Extract only the model's latest answer
        if "A:" in generated_answer:
            answer = generated_answer.split("A:")[-1].strip()
        else:
            answer = generated_answer.strip()

        # Append the AI answer to the conversation history
        conversation_history.append(f"Q: {question}")
        conversation_history.append(f"A: {answer}")

        return answer

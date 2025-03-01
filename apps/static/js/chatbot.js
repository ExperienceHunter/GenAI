let modelSelector = document.getElementById("model-selector");
let chatBox = document.getElementById("chat-box");
let userInput = document.getElementById("user-input");
let sendButton = document.getElementById("send-button");

// Function to choose the model
function chooseModel() {
    const modelChoice = modelSelector.value;
    sendButton.disabled = false;  // Enable the send button once the model is selected

    fetch("/choose_model", {
        method: "POST",
        body: new URLSearchParams({ model_choice: modelChoice }),
        headers: { "Content-Type": "application/x-www-form-urlencoded" }
    })
    .then(response => response.json())
    .then(data => {
        if (data.status === "success") {
            addMessage(`You selected ${modelChoice}.`, "ai");
        } else {
            alert(data.message);
        }
    })
    .catch(error => console.error('Error:', error));
}

// Function to send a question
function sendQuestion() {
    const question = userInput.value.trim();
    if (!question) return;

    addMessage(`You: ${question}`, "user");
    userInput.value = "";
    disableInput(true);

    let loadingMessage = document.createElement("div");
    loadingMessage.classList.add("loader"); // Use spinner animation
    chatBox.appendChild(loadingMessage);
    chatBox.scrollTop = chatBox.scrollHeight;

    fetch("/ask_question", {
        method: "POST",
        body: new URLSearchParams({ question: question }),
        headers: { "Content-Type": "application/x-www-form-urlencoded" }
    })
    .then(response => response.json())
    .then(data => {
        loadingMessage.remove();
        if (data.status === "processing") {
            checkTaskStatus(data.task_id);
        } else if (data.status === "error") {
            addMessage(`Error: ${data.error}`, "error");
            disableInput(false);
        } else {
            addMessage(formatResponse(data.answer), "ai");
            disableInput(false);
        }
    })
    .catch(error => {
        loadingMessage.remove();
        addMessage("Error: An unexpected error occurred.", "error");
        disableInput(false);
    });
}

// Function to check the task status
function checkTaskStatus(taskId) {
    setTimeout(() => {
        fetch(`/get_answer/${taskId}`)
            .then(response => response.json())
            .then(data => {
                if (data.status === "processing") {
                    checkTaskStatus(taskId);
                } else {
                    addMessage(formatResponse(data.answer), "ai");
                    disableInput(false);
                }
            });
    }, 1000);
}

// Function to add messages to the chat
function addMessage(message, sender) {
    let messageDiv = document.createElement("div");
    messageDiv.classList.add(sender);
    messageDiv.innerHTML = message;
    chatBox.appendChild(messageDiv);
    chatBox.scrollTop = chatBox.scrollHeight;
}

// Function to format AI responses
function formatResponse(response) {
    if (!response) return "No response received.";
    let formattedMessage = response.trim();

    if (formattedMessage.match(/^\d+\./)) {
        let lines = formattedMessage.split('\n');
        let listItems = lines.map(line => `<li>${line.trim()}</li>`).join('');
        return `<ol>${listItems}</ol>`;
    }

    if (formattedMessage.includes("\n")) {
        let lines = formattedMessage.split('\n');
        return lines.map(line => `<p>${line.trim()}</p>`).join('');
    }

    return formattedMessage;
}

// Handle "Enter" key press
function checkEnter(event) {
    if (event.key === 'Enter') {
        sendQuestion();
    }
}

// Function to clear the chat (Fix: Also clears server-side history)
function clearChat() {
    if (confirm("Are you sure you want to clear the chat?")) {
        chatBox.innerHTML = "";  // Clear chat messages in UI

        fetch("/clear_chat", {
            method: "POST",
            headers: { "Content-Type": "application/x-www-form-urlencoded" }
        })
        .then(response => response.json())
        .then(data => {
            if (data.status === "success") {
                console.log("Chat history cleared.");
            } else {
                console.error("Failed to clear chat history.");
            }
        })
        .catch(error => console.error('Error:', error));
    }
}

// Disable or enable input fields
function disableInput(disable) {
    userInput.disabled = disable;
    sendButton.disabled = disable;
}

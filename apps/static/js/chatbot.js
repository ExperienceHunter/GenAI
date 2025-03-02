// Element References
const modelSelector = document.getElementById("model-selector");
const chatBox = document.getElementById("chat-box");
const userInput = document.getElementById("user-input");
const sendButton = document.getElementById("send-button");
const fileUploadButton = document.getElementById("file-upload-button");
const fileInput = document.getElementById("file-upload");
const fileSizeDisplay = document.getElementById('fileSize');
const clearFileButton = document.getElementById('clearFile');

// Event Listeners
modelSelector.addEventListener("change", chooseModel);
userInput.addEventListener("keydown", checkEnter);
fileUploadButton.addEventListener("click", uploadFile);
document.getElementById("file-upload-form").addEventListener("submit", handleFileFormSubmit);
fileInput.addEventListener("change", displayFileSize);
clearFileButton.addEventListener('click', clearFileInput);

// Function to choose the model
function chooseModel() {
    const modelChoice = modelSelector.value;
    sendButton.disabled = false;

    fetch("/choose_model", {
        method: "POST",
        body: new URLSearchParams({ model_choice: modelChoice }),
        headers: { "Content-Type": "application/x-www-form-urlencoded" }
    })
    .then(handleResponse)
    .catch(handleError);
}

// Function to send a question
function sendQuestion() {
    const question = userInput.value.trim();
    if (!question) return;

    addMessage(`You: ${question}`, "user");
    userInput.value = "";
    disableInput(true);

    const loadingMessage = createLoadingMessage();
    chatBox.appendChild(loadingMessage);
    chatBox.scrollTop = chatBox.scrollHeight;

    fetch("/ask_question", {
        method: "POST",
        body: new URLSearchParams({ question }),
        headers: { "Content-Type": "application/x-www-form-urlencoded" }
    })
    .then(handleQuestionResponse.bind(null, loadingMessage))
    .catch(handleError.bind(null, loadingMessage));
}

// Function to handle question response
function handleQuestionResponse(loadingMessage, response) {
    loadingMessage.remove();
    response.json().then(data => {
        if (data.status === "processing") {
            checkTaskStatus(data.task_id);
        } else {
            processQuestionResponse(data);
        }
    });
}

// Function to process the question's response
function processQuestionResponse(data) {
    if (data.status === "error") {
        addMessage(`Error: ${data.error}`, "error");
    } else {
        addMessage(formatResponse(data.answer), "ai");
    }
    disableInput(false);
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
    const messageDiv = document.createElement("div");
    messageDiv.classList.add(sender);
    messageDiv.innerHTML = message;
    chatBox.appendChild(messageDiv);
    chatBox.scrollTop = chatBox.scrollHeight;
}

// Function to format AI responses
function formatResponse(response) {
    if (!response) return "No response received.";
    const formattedMessage = response.trim();

    if (formattedMessage.match(/^\d+\./)) {
        return formatListResponse(formattedMessage);
    }

    if (formattedMessage.includes("\n")) {
        return formatParagraphResponse(formattedMessage);
    }

    return formattedMessage;
}

// Helper: Format numbered list responses
function formatListResponse(response) {
    const lines = response.split('\n');
    return `<ol>${lines.map(line => `<li>${line.trim()}</li>`).join('')}</ol>`;
}

// Helper: Format paragraph responses
function formatParagraphResponse(response) {
    return response.split('\n').map(line => `<p>${line.trim()}</p>`).join('');
}

// Handle "Enter" key press for sending questions
function checkEnter(event) {
    if (event.key === 'Enter') sendQuestion();
}

// Clear the chat history (UI and server-side)
function clearChat() {
    if (confirm("Are you sure you want to clear the chat?")) {
        chatBox.innerHTML = "";  // Clear UI chat messages
        fetch("/clear_chat", {
            method: "POST",
            headers: { "Content-Type": "application/x-www-form-urlencoded" }
        })
        .then(response => response.json())
        .then(data => {
            if (data.status !== "success") {
                console.error("Failed to clear chat history.");
            }
        })
        .catch(handleError);
    }
}

// Disable input fields (send button and user input)
function disableInput(disable) {
    userInput.disabled = disable;
    sendButton.disabled = disable;
}

// Helper: Create a loading message (spinner)
function createLoadingMessage() {
    const loadingMessage = document.createElement("div");
    loadingMessage.classList.add("loader");
    return loadingMessage;
}

// File Upload: Handle file selection and upload
function uploadFile() {
    const file = fileInput.files[0];
    if (!file) {
        alert("No file selected!");
        return;
    }

    const formData = new FormData();
    formData.append('file', file);

    const loadingMessage = createLoadingMessage();
    document.body.appendChild(loadingMessage);

    fetch("/upload_file", {
        method: "POST",
        body: formData
    })
    .then(handleFileUploadResponse.bind(null, loadingMessage))
    .catch(handleError.bind(null, loadingMessage));
}

// Handle the file upload form submission
function handleFileFormSubmit(event) {
    event.preventDefault();
    uploadFile();
}

// Handle the file upload response
function handleFileUploadResponse(loadingMessage, response) {
    loadingMessage.remove();
    response.json().then(data => {
        if (data.error) {
            alert("Error: " + data.error);
        } else {
            alert("File uploaded successfully!");
        }
    });
}

// Display selected file size
function displayFileSize(event) {
    const file = event.target.files[0];
    if (file) {
        const fileSize = (file.size / 1024 / 1024).toFixed(2);
        fileSizeDisplay.textContent = `File size: ${fileSize} MB`;
    } else {
        fileSizeDisplay.textContent = '';
    }
}

// Clear file input field and size display
function clearFileInput() {
    fileInput.value = '';
    fileSizeDisplay.textContent = '';
}

// Generic Error handler
function handleError(error) {
    console.error("Error:", error);
    alert("An unexpected error occurred.");
}

// Generic response handler
function handleResponse(response) {
    response.json().then(data => {
        if (data.status !== "success") {
            alert(data.message);
        }
    });
}

document.getElementById('vectorizeDataBtn').addEventListener('click', function() {
    console.log("Vectorize button clicked");  // Add this line to confirm the button is clicked

    // Proceed with the fetch request
    var messageElement = document.getElementById('message');
    messageElement.style.color = 'blue';
    messageElement.textContent = 'Vectorization in progress...';

    fetch('/vectorize', {
        method: 'POST',
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            messageElement.style.color = 'green';
            messageElement.textContent = 'Vectorization completed successfully!';
        } else {
            messageElement.style.color = 'red';
            messageElement.textContent = 'Error: ' + data.error;
        }
    })
    .catch(error => {
        messageElement.style.color = 'red';
        messageElement.textContent = 'Error: ' + error;
    });
});

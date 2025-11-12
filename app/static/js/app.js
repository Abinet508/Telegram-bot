// Global variables
let currentOperation = null;

// Utility functions
function showAlert(message, type = 'info') {
    const alertDiv = document.createElement('div');
    alertDiv.className = `alert alert-${type} alert-dismissible fade show`;
    alertDiv.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;
    document.querySelector('.container-fluid').insertBefore(alertDiv, document.querySelector('.container-fluid').firstChild);
}

function updateOperationProgress(data) {
    const progressBar = document.getElementById(`progress-${data.operation_id}`);
    if (progressBar) {
        progressBar.style.width = `${data.status.progress}%`;
        progressBar.textContent = `${data.status.progress}%`;
    }
}

function handleOperationComplete(data) {
    showAlert(`Operation completed successfully!`, 'success');
    currentOperation = null;
}

function handleOperationError(data) {
    showAlert(`Operation failed: ${data.error}`, 'danger');
    currentOperation = null;
}
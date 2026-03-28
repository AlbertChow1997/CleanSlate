const uploadForm = document.getElementById("uploadForm");
const csvFile = document.getElementById("csvFile");
const dropzone = document.getElementById("dropzone");
const dropzoneSubtitle = document.getElementById("dropzoneSubtitle");
const sampleButton = document.getElementById("sampleButton");
const submitButton = document.getElementById("submitButton");
const statusText = document.getElementById("statusText");
const resultsSection = document.getElementById("resultsSection");
const metricsGrid = document.getElementById("metricsGrid");
const notesList = document.getElementById("notesList");
const missingList = document.getElementById("missingList");
const previewTable = document.getElementById("previewTable");
const downloadCsvLink = document.getElementById("downloadCsvLink");
const reportLink = document.getElementById("reportLink");
const reportJsonLink = document.getElementById("reportJsonLink");
const executionModeBlurb = document.getElementById("executionModeBlurb");

csvFile.addEventListener("change", () => {
  updateSelectedFileState(csvFile.files[0] || null);
});

dropzone.addEventListener("dragover", (event) => {
  event.preventDefault();
  dropzone.classList.add("drag-active");
});

dropzone.addEventListener("dragleave", () => {
  dropzone.classList.remove("drag-active");
});

dropzone.addEventListener("drop", (event) => {
  event.preventDefault();
  dropzone.classList.remove("drag-active");
  const file = event.dataTransfer?.files?.[0];
  if (!file) {
    return;
  }
  if (!file.name.toLowerCase().endsWith(".csv")) {
    statusText.textContent = "Please drop a CSV file.";
    return;
  }

  const dataTransfer = new DataTransfer();
  dataTransfer.items.add(file);
  csvFile.files = dataTransfer.files;
  updateSelectedFileState(file);
});

sampleButton.addEventListener("click", async () => {
  try {
    const response = await fetch("/assets/sample_data/messy_sales_ops.csv");
    if (!response.ok) {
      throw new Error("Could not load the sample CSV.");
    }

    const blob = await response.blob();
    const file = new File([blob], "messy_sales_ops.csv", { type: "text/csv" });
    const dataTransfer = new DataTransfer();
    dataTransfer.items.add(file);
    csvFile.files = dataTransfer.files;
    updateSelectedFileState(file);
    statusText.textContent = "Sample CSV loaded and ready to process.";
  } catch (error) {
    statusText.textContent = error.message || "Could not load the sample CSV.";
  }
});

uploadForm.addEventListener("submit", async (event) => {
  event.preventDefault();

  if (!csvFile.files.length) {
    statusText.textContent = "Choose a CSV file to continue.";
    return;
  }

  statusText.textContent = "Creating cleanup job and analyzing your data...";
  submitButton.disabled = true;

  const formData = new FormData();
  formData.append("file", csvFile.files[0]);

  try {
    const response = await fetch("/api/process", {
      method: "POST",
      body: formData,
    });

    if (!response.ok) {
      let message = "Processing failed.";
      const contentType = response.headers.get("content-type") || "";
      if (contentType.includes("application/json")) {
        const error = await response.json();
        message = error.detail || message;
      } else {
        const text = await response.text();
        if (text) {
          message = text;
        }
      }
      throw new Error(message);
    }

    const payload = await response.json();
    renderResults(payload);
    statusText.textContent = "Cleanup complete. Review the report and download the cleaned CSV.";
  } catch (error) {
    statusText.textContent = error.message;
  } finally {
    submitButton.disabled = false;
  }
});

function updateSelectedFileState(file) {
  if (!file) {
    dropzoneSubtitle.textContent = "Customer exports, CRM dumps, order sheets, and more";
    return;
  }
  dropzoneSubtitle.textContent = `Selected file: ${file.name}`;
}

function renderResults(payload) {
  resultsSection.classList.remove("hidden");

  const { metrics, cleaned_preview: preview, execution_mode: executionMode } = payload;
  executionModeBlurb.textContent = executionMode === "daytona" ? "Executed in Daytona" : "Executed locally with Daytona-ready service";

  metricsGrid.innerHTML = "";
  const cards = [
    ["Rows before", metrics.rows_before],
    ["Rows after", metrics.rows_after],
    ["Duplicates removed", metrics.duplicates_removed],
    ["Emails normalized", metrics.emails_normalized],
    ["Phones normalized", metrics.phones_normalized],
    ["Dates standardized", metrics.dates_normalized],
  ];

  cards.forEach(([label, value]) => {
    const article = document.createElement("article");
    article.className = "metric-card";
    article.innerHTML = `<span>${label}</span><strong>${value}</strong>`;
    metricsGrid.appendChild(article);
  });

  notesList.innerHTML = "";
  metrics.notes.forEach((note) => {
    const item = document.createElement("li");
    item.textContent = note;
    notesList.appendChild(item);
  });

  missingList.innerHTML = "";
  Object.entries(metrics.missing_values_by_column).forEach(([column, count]) => {
    const item = document.createElement("li");
    item.textContent = `${column}: ${count}`;
    missingList.appendChild(item);
  });

  renderPreview(preview);

  downloadCsvLink.href = payload.cleaned_csv_url;
  reportLink.href = payload.report_html_url;
  reportJsonLink.href = payload.report_json_url;
}

function renderPreview(rows) {
  previewTable.innerHTML = "";
  if (!rows.length) {
    previewTable.innerHTML = "<tr><td>No preview rows available.</td></tr>";
    return;
  }

  const columns = Object.keys(rows[0]);
  const thead = document.createElement("thead");
  const headerRow = document.createElement("tr");
  columns.forEach((column) => {
    const cell = document.createElement("th");
    cell.textContent = column;
    headerRow.appendChild(cell);
  });
  thead.appendChild(headerRow);
  previewTable.appendChild(thead);

  const tbody = document.createElement("tbody");
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    columns.forEach((column) => {
      const td = document.createElement("td");
      td.textContent = row[column] ?? "";
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  previewTable.appendChild(tbody);
}

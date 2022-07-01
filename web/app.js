// Spindle Interactive Workflow Dashboard Controller

const API_BASE = "http://localhost:5000";

let state = {
    instances: [
        {
            id: "order-ord-8812",
            type: "Order Fulfillment Saga",
            status: "Running",
            currentStep: 2,
            events: [
                { seq: 1, ts: "2026-10-22T07:26:46Z", type: "WorkflowExecutionStarted", summary: "Tenant: default", hash: "0xD0C57D00" },
                { seq: 2, ts: "2026-10-22T07:26:47Z", type: "ActivityCompleted", summary: "AuthorizePayment ($249.99)", hash: "0xF6DD39C2" },
                { seq: 3, ts: "2026-10-22T07:26:47Z", type: "ActivityCompleted", summary: "ReserveInventory (WH-WEST)", hash: "0x06F895E7" }
            ],
            steps: [
                { name: "AuthorizePayment", status: "completed", compensation: "RefundPayment" },
                { name: "ReserveInventory", status: "completed", compensation: "ReleaseInventory" },
                { name: "DispatchCourier", status: "running", compensation: "CancelDispatch" },
                { name: "FinalizeOrder", status: "pending", compensation: null }
            ]
        }
    ],
    selectedId: "order-ord-8812",
    scrubIndex: 2
};

function init() {
    renderInstances();
    renderActiveWorkflow();
    setupEventListeners();
}

function setupEventListeners() {
    document.getElementById("btn-generate-id").addEventListener("click", () => {
        const rand = Math.floor(1000 + Math.random() * 9000);
        document.getElementById("wf-id-input").value = `order-ord-${rand}`;
    });

    document.getElementById("btn-start-workflow").addEventListener("click", () => {
        const wfType = document.getElementById("wf-type-select").value;
        const wfId = document.getElementById("wf-id-input").value.trim() || `order-${Date.now()}`;
        startWorkflow(wfType, wfId);
    });

    document.getElementById("btn-step-wf").addEventListener("click", () => {
        stepWorkflow();
    });

    document.getElementById("btn-simulate-failure").addEventListener("click", () => {
        simulateFailure();
    });

    const slider = document.getElementById("timeline-slider");
    slider.addEventListener("input", (e) => {
        const idx = parseInt(e.target.value, 10);
        state.scrubIndex = idx;
        document.getElementById("slider-step-indicator").textContent = `#${idx + 1}`;
        renderJournalTable();
    });
}

function startWorkflow(type, id) {
    const newInst = {
        id: id,
        type: type === "order_fulfillment" ? "Order Fulfillment Saga" : "Financial Multi-Leg Settlement",
        status: "Running",
        currentStep: 0,
        events: [
            { seq: 1, ts: new Date().toISOString(), type: "WorkflowExecutionStarted", summary: `Type: ${type}`, hash: "0x89A120EF" }
        ],
        steps: [
            { name: "AuthorizePayment", status: "running", compensation: "RefundPayment" },
            { name: "ReserveInventory", status: "pending", compensation: "ReleaseInventory" },
            { name: "DispatchCourier", status: "pending", compensation: "CancelDispatch" },
            { name: "FinalizeOrder", status: "pending", compensation: null }
        ]
    };
    state.instances.unshift(newInst);
    state.selectedId = id;
    state.scrubIndex = 0;
    renderInstances();
    renderActiveWorkflow();
}

function stepWorkflow() {
    const inst = state.instances.find(i => i.id === state.selectedId);
    if (!inst || inst.status !== "Running") return;

    if (inst.currentStep < inst.steps.length - 1) {
        inst.steps[inst.currentStep].status = "completed";
        inst.currentStep++;
        inst.steps[inst.currentStep].status = "running";

        const seq = inst.events.length + 1;
        inst.events.push({
            seq: seq,
            ts: new Date().toISOString(),
            type: "ActivityCompleted",
            summary: `Step: ${inst.steps[inst.currentStep - 1].name}`,
            hash: "0x" + Math.floor(Math.random() * 0xFFFFFFFF).toString(16).toUpperCase()
        });
    } else {
        inst.steps[inst.currentStep].status = "completed";
        inst.status = "Completed";
        const seq = inst.events.length + 1;
        inst.events.push({
            seq: seq,
            ts: new Date().toISOString(),
            type: "WorkflowExecutionCompleted",
            summary: "All steps fulfilled successfully",
            hash: "0x" + Math.floor(Math.random() * 0xFFFFFFFF).toString(16).toUpperCase()
        });
    }

    state.scrubIndex = inst.events.length - 1;
    renderInstances();
    renderActiveWorkflow();
}

function simulateFailure() {
    const inst = state.instances.find(i => i.id === state.selectedId);
    if (!inst || inst.status !== "Running") return;

    inst.status = "Compensating";
    inst.steps[inst.currentStep].status = "failed";

    const seq = inst.events.length + 1;
    inst.events.push({
        seq: seq,
        ts: new Date().toISOString(),
        type: "ActivityFailed",
        summary: `Carrier Timeout on ${inst.steps[inst.currentStep].name}`,
        hash: "0xE35C764F"
    });

    inst.events.push({
        seq: seq + 1,
        ts: new Date().toISOString(),
        type: "SagaCompensationStarted",
        summary: "LIFO compensation rollback initiated",
        hash: "0xA98124BB"
    });

    inst.status = "Compensated";
    state.scrubIndex = inst.events.length - 1;
    renderInstances();
    renderActiveWorkflow();
}

function renderInstances() {
    const container = document.getElementById("instances-list");
    container.innerHTML = "";

    state.instances.forEach(inst => {
        const item = document.createElement("div");
        item.className = `instance-item ${inst.id === state.selectedId ? "active" : ""}`;
        item.onclick = () => {
            state.selectedId = inst.id;
            state.scrubIndex = inst.events.length - 1;
            renderInstances();
            renderActiveWorkflow();
        };

        const badgeClass = `badge-${inst.status.toLowerCase()}`;
        item.innerHTML = `
            <div class="instance-item-header">
                <span>${inst.id}</span>
                <span class="status-badge ${badgeClass}">${inst.status}</span>
            </div>
            <div class="instance-item-details">${inst.type} &bull; ${inst.events.length} Events</div>
        `;
        container.appendChild(item);
    });
}

function renderActiveWorkflow() {
    const inst = state.instances.find(i => i.id === state.selectedId);
    if (!inst) return;

    document.getElementById("active-wf-title").textContent = `${inst.id} (${inst.type})`;

    const slider = document.getElementById("timeline-slider");
    slider.max = inst.events.length - 1;
    slider.value = state.scrubIndex;
    document.getElementById("slider-step-indicator").textContent = `#${state.scrubIndex + 1}`;

    renderDag(inst);
    renderJournalTable();
}

function renderDag(inst) {
    const container = document.getElementById("dag-container");
    container.innerHTML = "";

    const svgWidth = Math.max(600, inst.steps.length * 150 + 100);
    const svgHeight = 260;

    let svg = `<svg width="${svgWidth}" height="${svgHeight}" xmlns="http://www.w3.org/2000/svg">`;

    // Start node
    svg += `
        <circle cx="50" cy="90" r="24" fill="#1f2937" stroke="#06b6d4" stroke-width="2"/>
        <text x="50" y="95" fill="#f9fafb" font-size="11" text-anchor="middle" font-family="monospace">Start</text>
    `;

    inst.steps.forEach((step, idx) => {
        const x = 160 + idx * 140;
        const prevX = idx === 0 ? 74 : (160 + (idx - 1) * 140 + 45);

        // Forward link
        svg += `<line x1="${prevX}" y1="90" x2="${x - 45}" y2="90" stroke="#374151" stroke-width="2" stroke-dasharray="4"/>`;

        let strokeColor = "#374151";
        let fillColor = "#111827";
        if (step.status === "completed") { strokeColor = "#10b981"; fillColor = "rgba(16, 185, 129, 0.15)"; }
        else if (step.status === "running") { strokeColor = "#3b82f6"; fillColor = "rgba(59, 130, 246, 0.2)"; }
        else if (step.status === "failed") { strokeColor = "#ef4444"; fillColor = "rgba(239, 68, 68, 0.2)"; }

        svg += `
            <rect x="${x - 45}" y="65" width="90" height="50" rx="6" fill="${fillColor}" stroke="${strokeColor}" stroke-width="2"/>
            <text x="${x}" y="88" fill="#f9fafb" font-size="10" font-weight="600" text-anchor="middle" font-family="sans-serif">${step.name}</text>
            <text x="${x}" y="104" fill="${strokeColor}" font-size="9" text-anchor="middle" font-family="monospace">${step.status.toUpperCase()}</text>
        `;

        // Compensation undo link
        if (step.compensation) {
            svg += `
                <path d="M ${x} 115 L ${x} 160" stroke="#f59e0b" stroke-width="1.5" stroke-dasharray="3"/>
                <rect x="${x - 40}" y="160" width="80" height="32" rx="4" fill="#1f2937" stroke="#f59e0b" stroke-width="1"/>
                <text x="${x}" y="176" fill="#f59e0b" font-size="8.5" text-anchor="middle" font-family="monospace">undo: ${step.compensation.slice(0, 10)}</text>
                <text x="${x}" y="187" fill="#9ca3af" font-size="7.5" text-anchor="middle" font-family="sans-serif">LIFO Rollback</text>
            `;
        }
    });

    svg += `</svg>`;
    container.innerHTML = svg;
}

function renderJournalTable() {
    const inst = state.instances.find(i => i.id === state.selectedId);
    const tbody = document.getElementById("journal-tbody");
    tbody.innerHTML = "";

    if (!inst) return;

    const visibleEvents = inst.events.slice(0, state.scrubIndex + 1);
    visibleEvents.forEach(evt => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
            <td>#${evt.seq}</td>
            <td>${evt.ts}</td>
            <td style="color: ${getEventColor(evt.type)}">${evt.type}</td>
            <td style="color: #9ca3af">${evt.summary}</td>
            <td style="color: #06b6d4">${evt.hash}</td>
        `;
        tbody.appendChild(tr);
    });
}

function getEventColor(type) {
    if (type.includes("Completed")) return "#10b981";
    if (type.includes("Failed")) return "#ef4444";
    if (type.includes("Started")) return "#3b82f6";
    if (type.includes("Compensat")) return "#f59e0b";
    return "#f9fafb";
}

document.addEventListener("DOMContentLoaded", init);

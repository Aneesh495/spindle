// Browser event journal and saga walkthrough. Each graph is derived from the
// prefix selected in the journal, so replay and forward execution agree.

const workflows = {
    order_fulfillment: {
        title: "Order fulfillment",
        steps: [
            ["Authorize payment", "Refund payment"],
            ["Reserve inventory", "Release inventory"],
            ["Dispatch courier", "Cancel dispatch"],
            ["Finalize order", null]
        ]
    },
    cloud_provisioning: {
        title: "Cloud provisioning",
        steps: [
            ["Reserve capacity", "Release capacity"],
            ["Create network", "Remove network"],
            ["Deploy service", "Remove service"],
            ["Verify health", null]
        ]
    },
    settlement: {
        title: "Financial settlement",
        steps: [
            ["Validate trade", null],
            ["Reserve funds", "Release funds"],
            ["Book settlement", "Reverse booking"],
            ["Issue receipt", null]
        ]
    }
};
const state = { instances: [], selectedId: null, scrubIndex: 0 };
const active = () => state.instances.find(instance => instance.id === state.selectedId);

function append(instance, type, summary, step = null) {
    instance.events.push({ seq: instance.events.length + 1, ts: new Date().toISOString(), type, summary, step });
}

function project(instance, count) {
    const steps = workflows[instance.type].steps.map(([name, compensation], index) => ({
        name, compensation, status: index === 0 ? "running" : "pending"
    }));
    let status = "Running";
    for (const event of instance.events.slice(0, count)) {
        if (event.type === "ActivityCompleted") {
            steps[event.step].status = "completed";
            if (steps[event.step + 1]) steps[event.step + 1].status = "running";
        } else if (event.type === "ActivityFailed") {
            steps[event.step].status = "failed";
        } else if (event.type === "ActivityCompensated") {
            steps[event.step].status = "compensated";
        } else if (event.type === "WorkflowExecutionCompleted") {
            status = "Completed";
        } else if (event.type === "WorkflowCompensated") {
            status = "Compensated";
        }
    }
    return { status, steps };
}

function init() {
    document.getElementById("btn-generate-id").addEventListener("click", () => {
        document.getElementById("wf-id-input").value = `workflow-${state.instances.length + 1}`;
    });
    document.getElementById("btn-start-workflow").addEventListener("click", () => {
        const type = document.getElementById("wf-type-select").value;
        const input = document.getElementById("wf-id-input");
        const id = input.value.trim() || `workflow-${state.instances.length + 1}`;
        const notice = document.getElementById("workflow-notice");
        if (state.instances.some(instance => instance.id === id)) {
            notice.textContent = "That instance ID is already in use.";
            input.focus();
            return;
        }
        notice.textContent = "";
        start(type, id);
        input.value = "";
    });
    document.getElementById("btn-step-wf").addEventListener("click", stepWorkflow);
    document.getElementById("btn-simulate-failure").addEventListener("click", simulateFailure);
    document.getElementById("timeline-slider").addEventListener("input", event => {
        state.scrubIndex = Number(event.target.value);
        renderActiveWorkflow();
    });
    start("order_fulfillment", "order-ord-8812");
    stepWorkflow();
    stepWorkflow();
}

function start(type, id) {
    const instance = { id, type, events: [] };
    append(instance, "WorkflowExecutionStarted", workflows[type].title);
    state.instances.unshift(instance);
    state.selectedId = id;
    showLatest(instance);
}

function showLatest(instance) {
    state.scrubIndex = instance.events.length - 1;
    renderInstances();
    renderActiveWorkflow();
}

function stepWorkflow() {
    const instance = active();
    if (!instance) return;
    const current = project(instance, instance.events.length);
    const index = current.steps.findIndex(step => step.status === "running");
    if (current.status !== "Running" || index < 0) return;
    append(instance, "ActivityCompleted", current.steps[index].name, index);
    if (index === current.steps.length - 1) append(instance, "WorkflowExecutionCompleted", "All activities completed");
    showLatest(instance);
}

function simulateFailure() {
    const instance = active();
    if (!instance) return;
    const current = project(instance, instance.events.length);
    const index = current.steps.findIndex(step => step.status === "running");
    if (current.status !== "Running" || index < 0) return;
    append(instance, "ActivityFailed", current.steps[index].name, index);
    for (let previous = index - 1; previous >= 0; previous--) {
        const step = current.steps[previous];
        if (step.compensation) append(instance, "ActivityCompensated", step.compensation, previous);
    }
    append(instance, "WorkflowCompensated", "Reverse-order compensation complete");
    showLatest(instance);
}

function renderInstances() {
    const container = document.getElementById("instances-list");
    container.replaceChildren();
    for (const instance of state.instances) {
        const item = document.createElement("button");
        item.type = "button";
        item.className = "instance-item";
        item.setAttribute("aria-pressed", String(instance.id === state.selectedId));
        if (instance.id === state.selectedId) item.classList.add("active");
        const title = document.createElement("strong");
        title.textContent = instance.id;
        const detail = document.createElement("span");
        detail.className = "instance-item-details";
        detail.textContent = `${workflows[instance.type].title} · ${project(instance, instance.events.length).status} · ${instance.events.length} events`;
        item.append(title, detail);
        item.addEventListener("click", () => {
            state.selectedId = instance.id;
            showLatest(instance);
            container.querySelector('[aria-pressed="true"]')?.focus();
        });
        container.appendChild(item);
    }
}

function renderActiveWorkflow() {
    const instance = active();
    if (!instance) return;
    const count = state.scrubIndex + 1;
    const frame = project(instance, count);
    document.getElementById("active-wf-title").textContent = `${instance.id} · ${frame.status}`;
    const slider = document.getElementById("timeline-slider");
    slider.max = instance.events.length - 1;
    slider.value = state.scrubIndex;
    document.getElementById("slider-step-indicator").textContent = `${count} / ${instance.events.length}`;
    const canAdvance = count === instance.events.length && frame.status === "Running";
    document.getElementById("btn-step-wf").disabled = !canAdvance;
    document.getElementById("btn-simulate-failure").disabled = !canAdvance;
    renderDag(frame);
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
        else if (step.status === "compensated") { strokeColor = "#f59e0b"; fillColor = "rgba(245,158,11,.15)"; }
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
    const tbody = document.getElementById("journal-tbody");
    tbody.replaceChildren();
    for (const event of active().events.slice(0, state.scrubIndex + 1)) {
        const row = tbody.insertRow();
        for (const value of [event.seq, event.ts, event.type, event.summary]) {
            row.insertCell().textContent = value;
        }
    }
}

document.addEventListener("DOMContentLoaded", init);

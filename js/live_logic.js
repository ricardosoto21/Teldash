// --- 🛠️ CONFIGURACIÓN MANUAL DE COLUMNAS (Mapeo Seguro) ---
const colMap = {
    // Pon aquí el NOMBRE EXACTO de la columna que quieres leer del Excel
    hora: 'SubmitDate', 
    numero: 'PhoneNumber', 
    operador: 'Operator',
    vendor: 'CompanyName',
    ruta: 'SMPPAccountName',
    status: 'DLRStatus', 
    mensaje: 'SMSMessage',
    delay: 'delay'
};

// Función para cerrar el modal
function closeLive() {
    document.getElementById('liveModal').style.display = 'none';
}

// Cierra el modal si haces clic fuera
window.onclick = function(event) {
    const modal = document.getElementById('liveModal');
    if (event.target == modal) closeLive();
}

async function showLiveTraffic(pais) {
    const modal = document.getElementById('liveModal');
    const title = document.getElementById('live-title');
    const tbody = document.querySelector('#live-table tbody');

    modal.style.display = 'block';
    title.innerHTML = `📡 Tráfico en Vivo: ${pais}`;
    tbody.innerHTML = '<tr><td colspan="5" style="text-align:center; padding: 20px;">Cargando registros...⏳</td></tr>';

    try {
        // Obtenemos la ruta correcta (datos/live_traffic.xlsx está en la raíz)
        const rutaDatos = "datos/live_traffic.xlsx"; // Si estás en index.html o Destinos.html, esta es la ruta
        
        // Cache busting con timestamp
        const res = await fetch(`${rutaDatos}?t=${new Date().getTime()}`);
        if (!res.ok) throw new Error("Archivo no encontrado");
        
        const buffer = await res.arrayBuffer();
        const wb = XLSX.read(buffer, { type: 'array' });
        const liveData = XLSX.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]]);

        // Filtramos por país
        const filtered = liveData.filter(d => d.CountryRealName === pais);
        
        if (filtered.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" style="text-align:center; padding: 20px;">No hay registros recientes para este país.</td></tr>';
            return;
        }

// --- 🛠️ GENERACIÓN DE FILAS ---
tbody.innerHTML = filtered.map(d => {
    // Extraemos los datos usando tus llaves manuales
    const horaRaw = d[colMap.hora] || '--:--:--';
    const horaSms = horaRaw.toString().split(' ')[1] || horaRaw;
    const numeroSms = d[colMap.numero] || 'N/A';
    const operadorSms = d[colMap.operador] || 'N/A';
    const vendorSms = d[colMap.vendor] || 'N/A'; // Nueva columna
    const rutaSms = d[colMap.ruta] || 'N/A';     // Nueva columna
    const statusSms = d[colMap.status] || 'Unknown';
    const mensajeSms = d[colMap.mensaje] || 'Sin contenido';
    const delaySms = d[colMap.delay] || '0s';    // Nueva columna

    // Color del status
    let statusColor = '#94a3b8';
    if(statusSms === 'Delivered') statusColor = '#10b981';
    else if(['Undelivered','Failed','Rejected'].includes(statusSms)) statusColor = '#ef4444';

    return `
        <tr>
            <td>${horaSms}</td>
            <td style="font-family: monospace;">${numeroSms}</td>
            <td>${operadorSms}</td>
            <td style="font-size: 11px;">${vendorSms}</td>
            <td style="font-size: 11px; color: var(--primary-color);">${rutaSms}</td>
            <td>
                <span style="background: ${statusColor}20; color: ${statusColor}; padding: 4px 8px; border-radius: 6px; font-weight: bold; font-size: 11px;">
                    ${statusSms}
                </span>
            </td>
            <td>${delaySms}</td>
            <td title="${mensajeSms}" style="max-width: 200px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; cursor: help; font-size: 11px;">
                ${mensajeSms}
            </td>
        </tr>
    `;
}).join('');
    } catch (e) {
        console.error("Error cargando live traffic:", e);
        tbody.innerHTML = '<tr><td colspan="5" style="text-align:center; color:#ef4444; padding: 20px;">Error: No se pudo cargar el archivo en vivo datos/live_traffic.xlsx.</td></tr>';
    }
}

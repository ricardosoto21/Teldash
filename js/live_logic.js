// --- 🛠️ CONFIGURACIÓN MANUAL DE COLUMNAS (Mapeo Seguro) ---
const colMap = {
    // Pon aquí el NOMBRE EXACTO de la columna que quieres leer del Excel
    hora: 'SubmitDate', 
    numero: 'PhoneNumber', 
    operador: 'Operator',
    cliente: 'CompanyName',
    ruta_cliente: 'SMPPAccountName',
    vendor:'VendorAccountName',   
    status: 'DLRStatus', 
    mensaje: 'SMSMessage',
    delay: 'DLRDelay'
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


        tbody.innerHTML = filtered.map(d => {
            // Extracción de datos según tu colMap
            const horaRaw = d[colMap.hora] || '--:--:--';
            const horaSms = horaRaw.toString().split(' ')[1] || horaRaw;
            const numeroSms = d[colMap.numero] || 'N/A';
            const operadorSms = d[colMap.operador] || 'N/A';
            const clienteSms = d[colMap.cliente] || 'N/A';
            const rutaClienteSms = d[colMap.ruta_cliente] || 'N/A';
            const vendorSms = d[colMap.vendor] || 'N/A';
            const statusSms = d[colMap.status] || 'Unknown';
            const delaySms = d[colMap.delay] !== undefined ? d[colMap.delay] + 's' : '0s';
            const mensajeSms = d[colMap.mensaje] || d.SMSMessage || d.Text || 'Sin contenido';

            // Colores dinámicos para el status
            let statusColor = '#94a3b8'; // gris por defecto
            if(statusSms === 'Delivered') statusColor = '#10b981'; // verde
            else if(['Undelivered','Failed','Rejected'].includes(statusSms)) statusColor = '#ef4444'; // rojo

            return `
                <tr>
                    <td>${horaSms}</td>
                    <td style="font-family: monospace; font-size: 13px;">${numeroSms}</td>
                    <td>${operadorSms}</td>
                    <td style="font-size: 11px; font-weight: 600; color: #6366f1;">${clienteSms}</td>
                    <td style="font-size: 11px;">${rutaClienteSms}</td>
                    <td style="font-size: 11px; font-weight: 600; color: #f59e0b;">${vendorSms}</td>
                    <td>
                        <span style="background: ${statusColor}20; color: ${statusColor}; padding: 4px 8px; border-radius: 6px; font-weight: bold; font-size: 11px;">
                            ${statusSms}
                        </span>
                    </td>
                    <td style="font-size: 12px;">${delaySms}</td>
                    <td title="${mensajeSms}" style="font-size: 11px; max-width: 250px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; cursor: help;">
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

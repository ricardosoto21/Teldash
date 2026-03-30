// --- 🛠️ CONFIGURACIÓN MANUAL DE COLUMNAS ---
const colMap = {
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

function closeLive() {
    document.getElementById('liveModal').style.display = 'none';
}

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
    tbody.innerHTML = '<tr><td colspan="9" style="text-align:center; padding: 20px;">Cargando registros...⏳</td></tr>';

    try {
        const rutaDatos = "datos/live_traffic.xlsx"; 
        const res = await fetch(`${rutaDatos}?t=${new Date().getTime()}`);
        if (!res.ok) throw new Error("Archivo no encontrado");
        
        const buffer = await res.arrayBuffer();
        const wb = XLSX.read(buffer, { type: 'array' });
        // Usamos raw: false para intentar que SheetJS mantenga el formato de texto de Excel
        const liveData = XLSX.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]], { raw: false });

        const filtered = liveData.filter(d => d.CountryRealName === pais);
        
        if (filtered.length === 0) {
            tbody.innerHTML = '<tr><td colspan="9" style="text-align:center; padding: 20px;">No hay registros recientes para este país.</td></tr>';
            return;
        }

        tbody.innerHTML = filtered.map(d => {
            // 🎯 UPGRADE 2: CORRECCIÓN DE FECHA (Maneja tanto formato serial de Excel como texto)
            let horaRaw = d[colMap.hora];
            let horaSms = '--:--:--';
            if (horaRaw) {
                if (!isNaN(horaRaw) && typeof horaRaw === 'number') {
                    let date = new Date((horaRaw - 25569) * 86400 * 1000);
                    date = new Date(date.getTime() + date.getTimezoneOffset() * 60000); // Ajuste de zona horaria
                    horaSms = date.toTimeString().split(' ')[0];
                } else {
                    horaSms = horaRaw.toString().includes(' ') ? horaRaw.toString().split(' ')[1] : horaRaw.toString();
                }
            }

            const numeroSms = d[colMap.numero] || 'N/A';
            const operadorSms = d[colMap.operador] || 'N/A';
            const clienteSms = d[colMap.cliente] || 'N/A';
            const rutaClienteSms = d[colMap.ruta_cliente] || 'N/A';
            const vendorSms = d[colMap.vendor] || 'N/A';
            const statusSms = d[colMap.status] || 'Unknown';
            const delaySms = d[colMap.delay] !== undefined ? d[colMap.delay] + 's' : '0s';
            const mensajeSms = d[colMap.mensaje] || d.Text || d.Message || 'Sin contenido';

            let statusColor = '#94a3b8';
            if(statusSms === 'Delivered') statusColor = '#10b981';
            else if(['Undelivered','Failed','Rejected'].includes(statusSms)) statusColor = '#ef4444';

            // 🎯 UPGRADE 3: SCROLL LATERAL PARA EL MENSAJE
            // Envolvemos el texto en un div con overflow-x: auto
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
                    <td>
                        <div style="max-width: 250px; overflow-x: auto; white-space: nowrap; padding-bottom: 5px; scrollbar-width: thin; font-size: 11px;">
                            ${mensajeSms}
                        </div>
                    </td>
                </tr>
            `;
        }).join('');
    } catch (e) {
        console.error("Error cargando live traffic:", e);
        tbody.innerHTML = '<tr><td colspan="9" style="text-align:center; color:#ef4444; padding: 20px;">Error: No se pudo cargar el archivo en vivo.</td></tr>';
    }
}

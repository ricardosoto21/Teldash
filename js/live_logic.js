// --- CONFIGURACIÓN DE COLUMNAS (Ajustado a tu proveedor) ---
const COL_MSG = 'Message'; 

// Función para cerrar el modal
function closeLive() {
    document.getElementById('liveModal').style.display = 'none';
}

// Cierra el modal si haces clic fuera de él
window.onclick = function(event) {
    const modal = document.getElementById('liveModal');
    if (event.target == modal) {
        closeLive();
    }
}

// Descarga y filtra el Excel de tráfico en vivo
async function showLiveTraffic(pais) {
    const modal = document.getElementById('liveModal');
    const title = document.getElementById('live-title');
    const tbody = document.querySelector('#live-table tbody');

    modal.style.display = 'block';
    title.innerHTML = `<span class="material-icons-outlined" style="color: #ef4444; animation: pulse 1.5s infinite;">sensors</span> Tráfico Reciente: ${pais}`;
    tbody.innerHTML = '<tr><td colspan="5" style="text-align:center; padding: 30px;">Cargando registros del servidor... ⏳</td></tr>';

    try {
        // Añadimos un timestamp para evitar la caché del navegador
        const res = await fetch(`datos/live_traffic.xlsx?t=${new Date().getTime()}`);
        if (!res.ok) throw new Error("Archivo no encontrado");
        
        const buffer = await res.arrayBuffer();
        const wb = XLSX.read(buffer, { type: 'array' });
        const liveData = XLSX.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]]);

        const filtered = liveData.filter(d => d.CountryRealName === pais);
        
        if (filtered.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" style="text-align:center; padding: 20px;">No hay tráfico registrado en las últimas horas para este destino.</td></tr>';
            return;
        }

        tbody.innerHTML = filtered.map(d => {
            const msg = d[COL_MSG] || d.SMSMessage || d.Text || 'Sin contenido';
            const hora = d.SubmitDate ? d.SubmitDate.toString().split(' ')[1] : '--:--';
            
            // Colores dinámicos para el status basados en tus variables CSS o colores fijos
            let statusColor = '#94a3b8'; // default gris
            if(d.DLRStatus === 'Delivered') statusColor = '#10b981'; // success verde
            else if(d.DLRStatus === 'Undelivered' || d.DLRStatus === 'Failed' || d.DLRStatus === 'Rejected') statusColor = '#ef4444'; // danger rojo

            return `
                <tr>
                    <td>${hora}</td>
                    <td style="font-family: monospace; font-size: 14px;">${d.PhoneNumber || d.Destination || 'N/A'}</td>
                    <td>${d.OperatorName || 'N/A'}</td>
                    <td>
                        <span style="background: ${statusColor}20; color: ${statusColor}; padding: 4px 8px; border-radius: 6px; font-weight: bold; font-size: 11px;">
                            ${d.DLRStatus}
                        </span>
                    </td>
                    <td title="${msg}" style="max-width: 300px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; cursor: help;">
                        ${msg}
                    </td>
                </tr>
            `;
        }).join('');
    } catch (e) {
        console.error("Error cargando live traffic:", e);
        tbody.innerHTML = '<tr><td colspan="5" style="text-align:center; color:#ef4444; padding: 20px;">Error: No se encontró el archivo datos/live_traffic.xlsx. ¿Ya se ejecutó el bot de Live Traffic?</td></tr>';
    }
}

// --- VINCULACIÓN "NINJA" CON TU GRÁFICO EXISTENTE ---
function bindChartClick() {
    // Busca si 'charts' y 'charts.c' (tu gráfico de barras) ya fueron creados por index.html
    if (window.charts && window.charts.c) {
        // Le inyectamos el evento onclick a las opciones del gráfico
        window.charts.c.options.onClick = (e, elements) => {
            if (elements.length > 0) {
                const index = elements[0].index;
                const pais = window.charts.c.data.labels[index];
                showLiveTraffic(pais);
            }
        };
        // Refrescamos el gráfico para que aplique el evento
        window.charts.c.update();
        console.log("✅ Conexión Live-Chart establecida.");
    } else {
        // Si el gráfico aún no renderiza, vuelve a intentar en 500ms
        setTimeout(bindChartClick, 500);
    }
}

// Iniciar la vinculación cuando cargue el archivo
bindChartClick();

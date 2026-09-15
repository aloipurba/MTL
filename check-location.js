/*
  Script bridge: cek data lokasi terbaru dari MQTT, kirim notifikasi OneSignal
  jika ada data baru. Dijalankan oleh GitHub Actions setiap jam (jadwal cron).

  Status "terakhir dinotif" disimpan di file state.json di dalam repo ini
  sendiri (bukan pakai database eksternal), lalu di-commit balik oleh
  GitHub Actions setelah setiap run.
*/

const mqtt = require('mqtt');
const fetch = require('node-fetch');
const fs = require('fs');

// ======== KONFIGURASI dari Environment Variables (GitHub Secrets) ========
const MQTT_HOST = process.env.MQTT_HOST;
const MQTT_PORT = 8883;
const MQTT_USER = process.env.MQTT_USER;
const MQTT_PASS = process.env.MQTT_PASS;
const TOPIC_LOCATION = 'tracker/motor1/location';

const ONESIGNAL_APP_ID = process.env.ONESIGNAL_APP_ID;
const ONESIGNAL_API_KEY = process.env.ONESIGNAL_API_KEY;
const ONESIGNAL_EXTERNAL_ID = 'motor1_owner'; // harus SAMA PERSIS dengan yang di-set via OneSignal.login() di Flutter

const STATE_FILE = './state.json';

async function main() {
  console.log('Mulai pengecekan data lokasi terbaru...');

  const payload = await ambilLokasiTerakhir();

  if (!payload) {
    console.log('Tidak ada data lokasi ditemukan (timeout/tidak ada retained message).');
    return;
  }

  const data = JSON.parse(payload);
  const timestampBaru = data.ts;

  if (!timestampBaru) {
    console.log('Data tidak punya field ts, dilewati.');
    return;
  }

  const lastNotified = bacaStateTerakhir();

  if (timestampBaru <= lastNotified) {
    console.log(`Data belum berubah (ts=${timestampBaru}, terakhir dinotif=${lastNotified}). Tidak kirim notif.`);
    return;
  }

  await kirimNotifikasi(data);
  simpanStateTerakhir(timestampBaru);

  console.log('Notifikasi berhasil dikirim untuk data ts=' + timestampBaru);
}

// ======== BACA/SIMPAN STATE DARI FILE LOKAL ========
function bacaStateTerakhir() {
  try {
    const isi = fs.readFileSync(STATE_FILE, 'utf8');
    const json = JSON.parse(isi);
    return json.ts || 0;
  } catch (err) {
    return 0; // file belum ada / rusak, anggap belum pernah notif
  }
}

function simpanStateTerakhir(ts) {
  fs.writeFileSync(STATE_FILE, JSON.stringify({ ts }, null, 2));
}

// ======== AMBIL RETAINED MESSAGE DARI MQTT ========
function ambilLokasiTerakhir() {
  return new Promise((resolve, reject) => {
    const client = mqtt.connect(`mqtts://${MQTT_HOST}:${MQTT_PORT}`, {
      username: MQTT_USER,
      password: MQTT_PASS,
      connectTimeout: 8000,
    });

    let hasilPayload = null;

    const timeout = setTimeout(() => {
      client.end(true);
      resolve(hasilPayload);
    }, 5000);

    client.on('connect', () => {
      client.subscribe(TOPIC_LOCATION, { qos: 1 });
    });

    client.on('message', (topic, message) => {
      if (topic === TOPIC_LOCATION) {
        hasilPayload = message.toString();
        clearTimeout(timeout);
        client.end(true);
        resolve(hasilPayload);
      }
    });

    client.on('error', (err) => {
      clearTimeout(timeout);
      client.end(true);
      reject(err);
    });
  });
}

// ======== KIRIM NOTIFIKASI VIA ONESIGNAL ========
async function kirimNotifikasi(data) {
  const response = await fetch('https://api.onesignal.com/notifications', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Key ${ONESIGNAL_API_KEY}`,
    },
    body: JSON.stringify({
      app_id: ONESIGNAL_APP_ID,
      include_aliases: { external_id: [ONESIGNAL_EXTERNAL_ID] },
      target_channel: 'push',
      headings: { en: 'Lokasi Motor Diperbarui' },
      contents: { en: 'Update lokasi terbaru diterima.' },
    }),
  });

  const result = await response.json();
  console.log('Respon OneSignal:', JSON.stringify(result));

  if (!response.ok) {
    throw new Error('Gagal kirim notifikasi: ' + JSON.stringify(result));
  }
}

main().catch((err) => {
  console.error('Error:', err);
  process.exit(1);
});

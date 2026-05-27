const express = require('express');
const cors = require('cors');
const http = require('http' );
const socketIo = require('socket.io');
const mysql = require('mysql2/promise');
require('dotenv').config();

const app = express();
const server = http.createServer(app );
const io = socketIo(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json());

// Pool de conexão com banco de dados
const pool = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

// ==================== MENSAGENS ====================

// Enviar mensagem
app.post('/api/mensagens', async (req, res) => {
  try {
    const { tipo, usuario, conteudo } = req.body;
    const timestamp = Date.now();
    
    if (!tipo || !usuario || !conteudo) {
      return res.status(400).json({ erro: 'Faltam campos obrigatórios' });
    }
    
    const connection = await pool.getConnection();
    
    const [resultado] = await connection.query(
      'INSERT INTO mensagens (tipo, usuario, conteudo, timestamp) VALUES (?, ?, ?, ?)',
      [tipo, usuario, conteudo, timestamp]
    );
    
    const mensagem = {
      id: resultado.insertId,
      tipo,
      usuario,
      conteudo,
      timestamp
    };
    
    connection.release();
    
    // Enviar para todos os clientes conectados
    io.emit('nova_mensagem', mensagem);
    
    res.json({ sucesso: true, mensagem });
  } catch (erro) {
    console.error('Erro ao enviar mensagem:', erro);
    res.status(500).json({ erro: 'Erro ao enviar mensagem' });
  }
});

// Obter todas as mensagens
app.get('/api/mensagens', async (req, res) => {
  try {
    const connection = await pool.getConnection();
    
    const [mensagens] = await connection.query(
      'SELECT * FROM mensagens ORDER BY timestamp DESC LIMIT 100'
    );
    
    connection.release();
    
    res.json(mensagens);
  } catch (erro) {
    console.error('Erro ao obter mensagens:', erro);
    res.status(500).json({ erro: 'Erro ao obter mensagens' });
  }
});

// ==================== MÉTRICAS ====================

// Registrar métrica
app.post('/api/metricas', async (req, res) => {
  try {
    const { rtt, throughput, packet_loss } = req.body;
    const timestamp = Date.now();
    
    const connection = await pool.getConnection();
    
    await connection.query(
      'INSERT INTO metricas (rtt, throughput, packet_loss, timestamp) VALUES (?, ?, ?, ?)',
      [rtt, throughput, packet_loss, timestamp]
    );
    
    connection.release();
    
    // Verificar alertas
    verificarAlertas(rtt, throughput, packet_loss);
    
    res.json({ sucesso: true });
  } catch (erro) {
    console.error('Erro ao registrar métrica:', erro);
    res.status(500).json({ erro: 'Erro ao registrar métrica' });
  }
});

// Obter últimas métricas
app.get('/api/metricas', async (req, res) => {
  try {
    const connection = await pool.getConnection();
    
    const [metricas] = await connection.query(
      'SELECT * FROM metricas ORDER BY timestamp DESC LIMIT 50'
    );
    
    connection.release();
    
    res.json(metricas);
  } catch (erro) {
    console.error('Erro ao obter métricas:', erro);
    res.status(500).json({ erro: 'Erro ao obter métricas' });
  }
});

// ==================== ALERTAS ====================

async function verificarAlertas(rtt, throughput, packet_loss) {
  try {
    const connection = await pool.getConnection();
    const timestamp = Date.now();
    
    // Verificar RTT
    if (rtt > 100) {
      await connection.query(
        'INSERT INTO alertas (tipo, valor, threshold, timestamp) VALUES (?, ?, ?, ?)',
        ['RTT Alto', rtt, 100, timestamp]
      );
      
      io.emit('novo_alerta', {
        tipo: 'RTT Alto',
        valor: rtt,
        threshold: 100,
        timestamp
      });
    }
    
    // Verificar Throughput
    if (throughput < 500) {
      await connection.query(
        'INSERT INTO alertas (tipo, valor, threshold, timestamp) VALUES (?, ?, ?, ?)',
        ['Throughput Baixo', throughput, 500, timestamp]
      );
      
      io.emit('novo_alerta', {
        tipo: 'Throughput Baixo',
        valor: throughput,
        threshold: 500,
        timestamp
      });
    }
    
    // Verificar Packet Loss
    if (packet_loss > 1) {
      await connection.query(
        'INSERT INTO alertas (tipo, valor, threshold, timestamp) VALUES (?, ?, ?, ?)',
        ['Packet Loss Alto', packet_loss, 1, timestamp]
      );
      
      io.emit('novo_alerta', {
        tipo: 'Packet Loss Alto',
        valor: packet_loss,
        threshold: 1,
        timestamp
      });
    }
    
    connection.release();
  } catch (erro) {
    console.error('Erro ao verificar alertas:', erro);
  }
}

// Obter alertas
app.get('/api/alertas', async (req, res) => {
  try {
    const connection = await pool.getConnection();
    
    const [alertas] = await connection.query(
      'SELECT * FROM alertas ORDER BY timestamp DESC LIMIT 50'
    );
    
    connection.release();
    
    res.json(alertas);
  } catch (erro) {
    console.error('Erro ao obter alertas:', erro);
    res.status(500).json({ erro: 'Erro ao obter alertas' });
  }
});

// ==================== WEBSOCKET ====================

io.on('connection', (socket) => {
  console.log('✅ Novo cliente conectado:', socket.id);
  
  socket.on('disconnect', () => {
    console.log('❌ Cliente desconectado:', socket.id);
  });
});

// ==================== INICIAR SERVIDOR ====================

server.listen(PORT, () => {
  console.log(`\n✅ Servidor rodando em http://localhost:${PORT}` );
  console.log(`📊 WebSocket conectado`);
  console.log(`💾 Banco de dados conectado\n`);
});

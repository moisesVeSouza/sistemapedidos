const express = require('express');
const { PubSub } = require('@google-cloud/pubsub');
const client = require('./db');

const app = express();
const pubSubClient = new PubSub({
    projectId: 'serjava-demo',
    keyFilename: './serjava-demo-1aceb58abd45.json'
});

const subscriptionName = 'projects/serjava-demo/subscriptions/marketing-sub';

app.get('/orders', async (req, res) => {
    const { uuid, cliente_id, produto_id } = req.query;

    let query = `
        SELECT p.*, c.nome AS cliente_nome, i.produto_id, i.quantidade
        FROM pedido p
        LEFT JOIN cliente c ON p.cliente_id = c.id
        LEFT JOIN item_do_pedido i ON p.id = i.pedido_id
        WHERE 1=1`;
        
    const params = [];
    let paramIndex = 1;

    // Filtrando por uuid
    if (uuid) {
        query += ` AND p.uuid = $${paramIndex}`;
        params.push(uuid);
        paramIndex++;
    }
    // Filtrando por cliente_id
    if (cliente_id) {
        query += ` AND p.cliente_id = $${paramIndex}`;
        params.push(cliente_id);
        paramIndex++;
    }
    // Filtrando por produto_id
    if (produto_id) {
        query += ` AND i.produto_id = $${paramIndex}`;
        params.push(produto_id);
    }

    try {
        const result = await client.query(query, params);
        // Agrupando os resultados por pedido para evitar duplicação
        const groupedResults = result.rows.reduce((acc, row) => {
            const { id, cliente_nome, produto_id, quantidade, ...pedidoData } = row;

            // Verifica se o pedido já está no acumulador
            if (!acc[id]) {
                acc[id] = { ...pedidoData, cliente_nome, itens: [] };
            }

            // Adiciona item ao pedido
            if (produto_id) {
                acc[id].itens.push({ produto_id, quantidade });
            }

            return acc;
        }, {});

        // Converte o objeto de volta em um array
        res.json(Object.values(groupedResults));
    } catch (err) {
        console.error(err);
        res.status(500).send('Erro ao consultar pedidos');
    }
});


async function listenForMessages() {
    const subscription = pubSubClient.subscription(subscriptionName);

    const messageHandler = async (message) => {
        console.log(`Mensagem recebida: ${message.id}`);
        let messageData = message.data.toString();
        console.log(`Dados da mensagem: ${messageData}`);

        let data;
        try {
            data = JSON.parse(messageData);
        } catch (err) {
            console.error(`Erro ao parsear mensagem JSON: ${err.message}`);
            message.nack();  // Não confirma a mensagem para que ela possa ser tratada novamente, se necessário
            return;
        }

        const { customer, items, uuid, created_at } = data;

        try {
            const clienteResult = await client.query(
                'INSERT INTO cliente (id, nome) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING RETURNING id',
                [customer.id, customer.name]
            );

            const clienteId = clienteResult.rows.length > 0 ? clienteResult.rows[0].id : customer.id;

            const valorTotal = items.reduce((total, item) => {
                return total + (item.sku.value * item.quantity);
            }, 0);

            const pedidoResult = await client.query(
                `INSERT INTO pedido (uuid, cliente_id, created_at, valor_total) 
                 VALUES ($1, $2, $3, $4) 
                 ON CONFLICT (uuid) DO UPDATE SET 
                 cliente_id = EXCLUDED.cliente_id,
                 created_at = EXCLUDED.created_at,
                 valor_total = EXCLUDED.valor_total
                 RETURNING id`,
                [uuid, clienteId, created_at, valorTotal]
            );

            const pedidoId = pedidoResult.rows[0].id;

            for (const item of items) {
                const produtoId = parseInt(item.sku.id, 10);
                if (isNaN(produtoId)) {
                    console.error(`Produto ID inválido: ${item.sku.id}`);
                    continue;
                }
                await client.query(
                    'INSERT INTO item_do_pedido (pedido_id, produto_id, quantidade) VALUES ($1, $2, $3)',
                    [pedidoId, produtoId, item.quantity]
                );
            }

            message.ack();
            console.log('Mensagem processada e armazenada com sucesso');
        } catch (err) {
            console.error('Erro ao armazenar mensagem:', err);
            message.nack();
        }
    };

    subscription.on('message', messageHandler);
    console.log(`Escutando mensagens na assinatura: ${subscriptionName}`);
}

listenForMessages().catch(console.error);

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
    console.log(`Servidor rodando na porta ${PORT}`);
});

# API Goomer - Produtos de Pedidos

Documentação para salvar e consultar produtos de pedidos do Goomer.

## 📋 Endpoints Disponíveis

### 1. Salvar Produtos de Pedidos

**Endpoint:** `POST /api/goomer/pedidos-produtos`

**Autenticação:** API Key (Header `X-API-Key`)

**Descrição:** Recebe uma lista de pedidos com seus produtos e salva no banco de dados. Se o pedido já existir, os produtos antigos são removidos e os novos são inseridos.

---

## 🔐 Autenticação

Este endpoint é protegido por **API Key** (não requer JWT):

```
X-API-Key: goomer_prod_2025_xyz789
```

---

## 📤 Request

### Headers

```
Content-Type: application/json
X-API-Key: goomer_prod_2025_xyz789
```

### Body (JSON)

```json
{
  "cod_branch": "0257",
  "pedidos": [
    {
      "numero_pedido": "1272",
      "produtos": [
        {
          "produto": "Coca-Cola Sem Açúcar",
          "quantidade": 1,
          "observacoes": []
        },
        {
          "produto": "Madero",
          "quantidade": 1,
          "observacoes": [
            "Maionese Separada"
          ]
        }
      ]
    },
    {
      "numero_pedido": "1273",
      "produtos": [
        {
          "produto": "Coca-Cola",
          "quantidade": 1,
          "observacoes": []
        },
        {
          "produto": "Cheese Mignon Bacon",
          "quantidade": 1,
          "observacoes": []
        }
      ]
    }
  ]
}
```

### Estrutura do Request

| Campo | Tipo | Obrigatório | Descrição |
|-------|------|-------------|-----------|
| `cod_branch` | string | Sim | Código da filial (ex: "0257") |
| `pedidos` | array | Sim | Lista de pedidos com produtos |

#### Estrutura de um Pedido

| Campo | Tipo | Obrigatório | Descrição |
|-------|------|-------------|-----------|
| `numero_pedido` | string | Sim | Número do pedido (ex: "1272") |
| `produtos` | array | Sim | Lista de produtos do pedido |

#### Estrutura de um Produto

| Campo | Tipo | Obrigatório | Descrição |
|-------|------|-------------|-----------|
| `produto` | string | Sim | Nome do produto |
| `quantidade` | integer | Sim | Quantidade do produto |
| `observacoes` | array[string] | Não | Lista de observações (ex: ["Sem alface", "Maionese extra"]) |

---

## 📥 Response

### Sucesso (201 Created)

```json
{
  "success": true,
  "saved_new": 4,
  "updated_existing": 2,
  "cod_branch": "0257"
}
```

### Campos da Resposta

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `success` | boolean | Indica se a operação foi bem-sucedida |
| `saved_new` | integer | Quantidade de produtos novos salvos |
| `updated_existing` | integer | Quantidade de produtos antigos removidos (quando o pedido já existia) |
| `cod_branch` | string | Código da filial processada |

### Erro (400/401/500)

```json
{
  "detail": "Mensagem de erro descritiva"
}
```

---

## 📖 Exemplos de Uso

### Exemplo 1: Salvar um pedido simples

```bash
curl -X POST "http://localhost:8000/api/goomer/pedidos-produtos" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: goomer_prod_2025_xyz789" \
  -d '{
    "cod_branch": "0257",
    "pedidos": [
      {
        "numero_pedido": "1272",
        "produtos": [
          {
            "produto": "Coca-Cola Sem Açúcar",
            "quantidade": 1,
            "observacoes": []
          }
        ]
      }
    ]
  }'
```

### Exemplo 2: Salvar múltiplos pedidos com observações

```bash
curl -X POST "http://localhost:8000/api/goomer/pedidos-produtos" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: goomer_prod_2025_xyz789" \
  -d '{
    "cod_branch": "0257",
    "pedidos": [
      {
        "numero_pedido": "1274",
        "produtos": [
          {
            "produto": "Choripán",
            "quantidade": 1,
            "observacoes": [
              "Maionese Extra",
              "Sem Conserva de Cebola",
              "Sem Alface"
            ]
          },
          {
            "produto": "Suco de Uva",
            "quantidade": 1,
            "observacoes": []
          }
        ]
      }
    ]
  }'
```

### Exemplo 3: Usando JavaScript/TypeScript (Fetch API)

```javascript
async function salvarProdutosPedidos(codBranch, pedidos) {
  const response = await fetch('http://localhost:8000/api/goomer/pedidos-produtos', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-API-Key': 'goomer_prod_2025_xyz789'
    },
    body: JSON.stringify({
      cod_branch: codBranch,
      pedidos: pedidos
    })
  });

  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.detail || 'Erro ao salvar produtos');
  }

  return await response.json();
}

// Uso
const pedidos = [
  {
    numero_pedido: "1272",
    produtos: [
      {
        produto: "Coca-Cola Sem Açúcar",
        quantidade: 1,
        observacoes: []
      }
    ]
  }
];

salvarProdutosPedidos("0257", pedidos)
  .then(result => {
    console.log('✅ Produtos salvos:', result);
  })
  .catch(error => {
    console.error('❌ Erro:', error);
  });
```

### Exemplo 4: Usando Python (requests)

```python
import requests
import json

url = "http://localhost:8000/api/goomer/pedidos-produtos"
headers = {
    "Content-Type": "application/json",
    "X-API-Key": "goomer_prod_2025_xyz789"
}

data = {
    "cod_branch": "0257",
    "pedidos": [
        {
            "numero_pedido": "1272",
            "produtos": [
                {
                    "produto": "Coca-Cola Sem Açúcar",
                    "quantidade": 1,
                    "observacoes": []
                },
                {
                    "produto": "Madero",
                    "quantidade": 1,
                    "observacoes": ["Maionese Separada"]
                }
            ]
        }
    ]
}

response = requests.post(url, headers=headers, json=data)

if response.status_code == 201:
    result = response.json()
    print(f"✅ Produtos salvos: {result['saved_new']} novos, {result['updated_existing']} atualizados")
else:
    error = response.json()
    print(f"❌ Erro: {error.get('detail', 'Erro desconhecido')}")
```

---

## ⚠️ Comportamento Importante

### Atualização de Pedidos Existentes

Quando você envia produtos para um pedido que **já existe** no banco:

1. **Todos os produtos antigos** desse pedido são **removidos**
2. **Novos produtos** são **inseridos**
3. O campo `updated_existing` na resposta indica quantos produtos foram removidos

**Exemplo:**

Se o pedido "1272" já tinha 3 produtos e você envia 2 novos produtos:
- Os 3 produtos antigos são removidos
- Os 2 novos produtos são inseridos
- `updated_existing: 3` e `saved_new: 2`

### Validações

- ✅ `cod_branch` é obrigatório
- ✅ `pedidos` deve ser um array não vazio
- ✅ Cada pedido deve ter `numero_pedido` e `produtos`
- ✅ Cada produto deve ter `produto` e `quantidade`
- ✅ `observacoes` é opcional (pode ser array vazio `[]`)

---

## 🔍 Consultar Produtos Salvos

Para consultar os produtos salvos, use o endpoint:

**GET** `/api/goomer/pedidos-produtos?cod_branch=0257&numero_pedido=1272`

**Autenticação:** JWT Token (Bearer Token)

**Exemplo:**

```bash
curl -X GET "http://localhost:8000/api/goomer/pedidos-produtos?cod_branch=0257&numero_pedido=1272" \
  -H "Authorization: Bearer SEU_TOKEN_JWT"
```

**Response:**

```json
{
  "success": true,
  "count": 2,
  "produtos": [
    {
      "id": 1,
      "numero_pedido": "1272",
      "produto": "Coca-Cola Sem Açúcar",
      "quantidade": 1,
      "observacoes": [],
      "cod_branch": "0257",
      "created_at": "2025-01-15T10:30:00",
      "updated_at": "2025-01-15T10:30:00"
    },
    {
      "id": 2,
      "numero_pedido": "1272",
      "produto": "Madero",
      "quantidade": 1,
      "observacoes": ["Maionese Separada"],
      "cod_branch": "0257",
      "created_at": "2025-01-15T10:30:00",
      "updated_at": "2025-01-15T10:30:00"
    }
  ]
}
```

---

## 🚨 Códigos de Erro

| Código | Descrição |
|--------|-----------|
| `201` | Produtos salvos com sucesso |
| `400` | Dados inválidos (campos obrigatórios faltando, formato incorreto) |
| `401` | API Key inválida ou ausente |
| `500` | Erro interno do servidor |

---

## 📝 Notas Importantes

1. **API Key:** A API Key deve ser enviada no header `X-API-Key`
2. **Formato de Data:** As datas são armazenadas em UTC no banco de dados
3. **Observações:** As observações são armazenadas como JSON array no banco
4. **Performance:** Para grandes volumes, envie múltiplos pedidos em uma única requisição
5. **Idempotência:** Enviar os mesmos dados múltiplas vezes é seguro (substitui os dados anteriores)

---

## 🔗 Endpoints Relacionados

- **Consultar produtos:** `GET /api/goomer/pedidos-produtos`
- **Timer de pedidos:** `POST /api/goomer/pedidos/iniciar-timer`
- **Liberar pedido:** `POST /api/goomer/pedidos/liberar`
- **Consultar tempo:** `GET /api/goomer/pedidos/{numero_pedido}/tempo`

---

## 📞 Suporte

Em caso de dúvidas ou problemas, verifique:
1. Se a API Key está correta
2. Se o formato JSON está válido
3. Se todos os campos obrigatórios estão presentes
4. Se o `cod_branch` corresponde a uma filial válida


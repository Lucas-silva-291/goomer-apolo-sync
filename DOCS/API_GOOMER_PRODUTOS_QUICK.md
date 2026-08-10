# API Goomer - Produtos de Pedidos (Guia Rápido)

## 🚀 Endpoint

```
POST /api/goomer/pedidos-produtos
```

## 🔑 Autenticação

```
X-API-Key: goomer_prod_2025_xyz789
```

## 📤 Exemplo de Request

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
          "observacoes": ["Maionese Separada"]
        }
      ]
    }
  ]
}
```

## 📥 Exemplo de Response

```json
{
  "success": true,
  "saved_new": 2,
  "updated_existing": 0,
  "cod_branch": "0257"
}
```

## 💻 Exemplo cURL

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

## ⚠️ Importante

- Se o pedido já existir, os produtos antigos são **removidos** e os novos são **inseridos**
- `observacoes` é opcional (pode ser `[]`)
- `quantidade` deve ser um número inteiro positivo

## 📚 Documentação Completa

Veja `API_GOOMER_PRODUTOS.md` para mais detalhes e exemplos.


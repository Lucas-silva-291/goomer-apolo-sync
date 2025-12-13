import bcrypt

senha_digitada = "232321"
hash_no_banco = "$2b$12$cgau0xjtAGeZitoCmQAHBuQWGkmWrzkG.4DW93/uhasIGccsD7L3a"

resultado = bcrypt.checkpw(
    senha_digitada.encode('utf-8'),
    hash_no_banco.encode('utf-8')
)
print(f"Senha correta? {resultado}")

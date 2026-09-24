bootstrap = dbutils.secrets.get(
    scope="cloudflow-kafka",
    key="bootstrap_servers"
)

username = dbutils.secrets.get(
    scope="cloudflow-kafka",
    key="username"
)

password = dbutils.secrets.get(
    scope="cloudflow-kafka",
    key="password"
)

ca_path = dbutils.secrets.get(
    scope="cloudflow-kafka",
    key="ca_certificate_path"
)

print(bootstrap)
print(username)
print(ca_path)
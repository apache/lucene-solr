package org.apache.solr.handler.dataimport;

import org.springframework.vault.authentication.TokenAuthentication;
import org.springframework.vault.client.VaultEndpoint;
import org.springframework.vault.core.VaultTemplate;
import org.springframework.vault.support.VaultResponse;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.*;

public class VaultServiceImpl {
    private static final String ENV_VAULT_TOKEN = "VAULT_TOKEN";
    private static final String ENV_VAULT_AUTH_PATH = "VAULT_AUTH_PATH";
    private static final String ENV_VAULT_ADDR = "VAULT_ADDR";
    private static final String ENV_VAULT_PATH = "VAULT_PATH";
    private static final String DATA = "data";
    private static final String vaultTokenFilePath = System.getenv().getOrDefault(ENV_VAULT_AUTH_PATH, "/etc/vault/token");
    private VaultTemplate vaultTemplate;
    private String vaultToken;
    private String vaultHost;

    public VaultServiceImpl() {
        try {
            vaultToken = getVaultToken();
            vaultHost = System.getenv(ENV_VAULT_ADDR);
            vaultTemplate = new VaultTemplate(VaultEndpoint.from(new URI(vaultHost)), new TokenAuthentication(vaultToken));
        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to vault using token", e);
        }
        watchVaultFileUpdates();
    }

    private String getVaultToken() {
        try {
            List<String> lines = Files.readAllLines(Paths.get(vaultTokenFilePath));
            if (!lines.isEmpty()) {
                return lines.get(0);
            } else {
                throw new RuntimeException(vaultTokenFilePath + ": vault auth token file is empty.");
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading vault auth token file", e);
        }
    }

    public Properties readVaultProperties() {
        String vaultPath = System.getenv(ENV_VAULT_PATH);
        if (vaultPath == null || vaultPath.length() == 0) {
            throw new RuntimeException(ENV_VAULT_PATH + " environment parameter is missing");
        }
        try {
            VaultResponse vaultResponse = vaultTemplate.read(vaultPath);
            return readVaultProperties(vaultResponse);
        } catch (Exception e) {
            // Do nothing
            throw new RuntimeException("readVaultProperties error", e);
        }
    }

    private static Properties readVaultProperties(VaultResponse vaultResponse) {
        if (vaultResponse != null && vaultResponse.getData() != null && vaultResponse.getData().get(DATA) != null) {
            Object obj = vaultResponse.getData().get(DATA);
            if (!(obj instanceof Map)) {
                return new Properties();
            }

            Map<String, Object> keyPairs = (Map<String, Object>)vaultResponse.getData().get(DATA);
            Properties properties = new Properties();
            for (Map.Entry<String, Object> entry : keyPairs.entrySet()) {
                properties.put(entry.getKey(), entry.getValue().toString());
            }
            return properties;
        }

        return new Properties();
    }

    private void watchVaultFileUpdates() {
        FileWatcher.getInstance().addListener(Paths.get(vaultTokenFilePath).getParent(), (path, kind) -> {
            vaultToken = getVaultToken();
            try {
                vaultTemplate = new VaultTemplate(VaultEndpoint.from(new URI(vaultHost)), new TokenAuthentication(vaultToken));
            } catch (Exception e) {
                throw new RuntimeException("vault configuration is wrong", e);
            }
        });
    }
}

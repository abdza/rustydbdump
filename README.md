# rustydbdump
MS SQL dump query into excel developed using rust

## Encrypted password

`password` in `settings.json` can be a jasypt `ENC(...)` value - the same one used
in cbmy-config.properties (PBEWITHHMACSHA512ANDAES_256). Set the master key in the
`JASYPT_ENCRYPTOR_PASSWORD` environment variable before running:

    JASYPT_ENCRYPTOR_PASSWORD=<master key> ./rustydbdump      (Linux)
    set JASYPT_ENCRYPTOR_PASSWORD=<master key>                (Windows cmd,
    rustydbdump.exe                                            no trailing space)

A plain-text password (not wrapped in `ENC(...)`) still works as before.

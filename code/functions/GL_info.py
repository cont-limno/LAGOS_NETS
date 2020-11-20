def GL(scale):
    if scale == 'Hr':
        GL_PID = ['{B97B71EB-32C0-48A1-804B-10EC7973FA2A}',
                  '{D93A3048-2CD8-4E81-9E4D-44A949E98BBB}',
                  '{1A1409A4-7C89-4254-98AD-9BB77DA5975A}',
                  '{0BB28A37-D665-4735-A2DA-5C08014F9EEF}',
                  '{35332563-13A5-4732-984B-9AE09BC72F00}',
                  '{6CED2D05-1374-4AF6-B933-D12311AD9653}',
                  '{48698991-B44C-4C9E-9C1D-87D63E2EDAE4}',
                  '{FEA9B66B-7C19-4008-A2ED-864591401427}',
                  '{ABC140E9-E3C6-4353-95C2-52B20F64919E}',
                  '{2BB43965-79CC-4260-AF47-DE8B48EC4DBB}',
                  '{10553BEA-1CEA-416B-9B79-8CB39E5E97E2}',
                  '{FF5C07A1-6796-4720-A157-306982AAA33C}',
                  '{C8E51F97-27D9-4E38-97A1-EA5C61450EC1}',
                  '{EAADA4EE-5389-4491-B92C-246FDBF8FBB6}',
                  '{0DED2BAE-A190-4E95-9744-45A208FCF723}',
                  '{B50E7201-1D6E-4FE8-ADED-497FFBBF1ED6}',
                  '{5669B427-827E-4E46-8B87-35717B15D944}',
                  '{14603A79-2F32-446C-A16B-F957EEE1B989}',
                  '{7BAAEBF0-EA14-45D8-8715-C5B7DD0825BB}',
                  '{92B44943-CA97-44AD-A8F5-F40FD57A28E8}',
                  '{DB49B412-732C-41D9-B246-E1253D7EA608}',
                  '{A723C6A1-A074-440E-8D8C-51B8C2CDA678}',
                  '{31FEF9E6-4DB9-43D1-A82C-B7D54CC43560}',
                  '{07E47584-B347-4C1B-BA24-878E82FA5975}',
                  '{908A600F-41A7-498C-B632-131A4263990E}',
                  '{D3AD59FA-5E12-4017-A30D-B76D9D2F90C9}',
                  '{3903D85F-5AF7-4D56-96CE-183BEEFA5AAC}',
                  '{E5735E0B-08B3-4AB3-A7F7-0552DC39D477}',
                  '156590089',
                  '{34E8DF18-8069-44E5-BC98-1436D2933A3D}',
                  '152712037']
    elif scale == 'Med':
        GL_PID = ['904140243',
                    '904140246',
                    '904140248',
                    '904140244',
                    '904140245']
    elif scale == 'Test':
        GL_PID = []
    return set(GL_PID)


def FCodeGL(scale):
    if scale == 'Test':
        return ['GL']
    else:
        return ['39004', '39010', '39000', '56600']


if __name__ == "__main__":
    pass
const DomainErrorTranslator = require('../DomainErrorTranslator');
const InvariantError = require('../InvariantError');

describe('DomainErrorTranslator', () => {
  it('should translate error correctly', () => {
    expect(DomainErrorTranslator
        .translate(new Error('REGISTER_USER.NOT_CONTAIN_NEEDED_PROPERTY')))
        .toStrictEqual(new InvariantError(
            // eslint-disable-next-line max-len
            'tidak dapat membuat user baru karena properti yang dibutuhkan tidak ada',
        ));
    expect(DomainErrorTranslator
        .translate(new Error('REGISTER_USER.NOT_MEET_DATA_TYPE_SPECIFICATION')))
        .toStrictEqual(new InvariantError(
            'tidak dapat membuat user baru karena tipe data tidak sesuai',
        ));
    expect(DomainErrorTranslator
        .translate(new Error('REGISTER_USER.USERNAME_LIMIT_CHAR')))
        .toStrictEqual(new InvariantError(
            // eslint-disable-next-line max-len
            'tidak dapat membuat user baru karena karakter username melebihi batas limit',
        ));
    expect(DomainErrorTranslator
        // eslint-disable-next-line max-len
        .translate(new Error('REGISTER_USER.USERNAME_CONTAIN_RESTRICTED_CHARACTER')))
        .toStrictEqual(new InvariantError(
            // eslint-disable-next-line max-len
            'tidak dapat membuat user baru karena username mengandung karakter terlarang',
        ));
  });

  // eslint-disable-next-line max-len
  it('should return original error when error message is not needed to translate', () => {
    // Arrange
    const error = new Error('some_error_message');

    // Action
    const translatedError = DomainErrorTranslator.translate(error);

    // Assert
    expect(translatedError).toStrictEqual(error);
  });
});

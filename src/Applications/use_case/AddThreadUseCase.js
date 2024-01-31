const AddThread = require('../../Domains/threads/entities/AddThread');

class AddThreadUseCase {
  constructor({ threadRepository }) {
    this._threadRepository = threadRepository;
  }

  async execute(payload) {
    const newThread = new AddThread(payload);
    return this._threadRepository.addThread(newThread);
  }
}

module.exports = AddThreadUseCase;

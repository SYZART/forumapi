const CommentRepository = require('../../../Domains/comments/CommentRepository');
const DeleteCommentUseCase = require('../DeleteCommentUseCase');

describe('DeleteCommentUseCase', () => {
  it('should orchestrating the delete comment action correctly', async () => {
    const useCasePayload = {
      commentId: 'comment-123',
      threadId: 'thread-123',
      userId: 'user-123',
    };

    const mockCommentRepository = new CommentRepository();

    mockCommentRepository.verifyAvailableCommentInThread = jest
        .fn(() => Promise.resolve());
    mockCommentRepository.verifyCommentOwner = jest.fn(() => Promise.resolve());
    mockCommentRepository.deleteCommentById = jest.fn(() => Promise.resolve());

    const deleteCommentUseCase = new DeleteCommentUseCase({
      commentRepository: mockCommentRepository,
    });

    await expect(deleteCommentUseCase.execute(useCasePayload))
        .resolves.not.toThrow();
    expect(mockCommentRepository.verifyAvailableCommentInThread)
        .toHaveBeenCalledWith(useCasePayload.commentId, useCasePayload.threadId);
    expect(mockCommentRepository.verifyCommentOwner)
        .toHaveBeenCalledWith(useCasePayload.commentId, useCasePayload.userId);
    expect(mockCommentRepository.deleteCommentById)
        .toHaveBeenCalledWith(useCasePayload.commentId);
  });
});

const ThreadRepository = require('../../../Domains/threads/ThreadRepository');
const CommentRepository = require('../../../Domains/comments/CommentRepository');
const ReplyRepository = require('../../../Domains/replies/ReplyRepository');
const DetailThread = require('../../../Domains/threads/entities/DetailThread');
const GetThreadByIdUseCase = require('../GetThreadByIdUseCase');

describe('GetThreadByIdUseCase', () => {
  it('should orchestrating the get thread by id action correctly', async () => {
    const useCasePayload = { id: 'thread-123' };
    const currentDate = new Date();
    const expectedThread = {
      id: 'thread-123',
      title: 'some thread',
      body: 'anything',
      date: currentDate,
      username: 'dicoding',
      comments: [
        {
          id: 'comment-123',
          username: 'dicoding',
          date: currentDate,
          content: 'x',
          replies: [
            {
              id: 'reply-123',
              content: 'some reply',
              date: currentDate,
              username: 'dicoding',
            },
          ],
        },
      ],
    };

    const mockThreadRepository = new ThreadRepository();
    const mockCommentRepository = new CommentRepository();
    const mockReplyRepository = new ReplyRepository();

    mockThreadRepository.getThreadById = jest.fn(() => Promise.resolve(
        new DetailThread({
          id: 'thread-123',
          title: 'some thread',
          body: 'anything',
          date: currentDate,
          username: 'dicoding',
        }),
    ));
    mockCommentRepository.getCommentsByThreadId = jest.fn(() => Promise.resolve(
        [
          {
            id: 'comment-123',
            username: 'dicoding',
            date: currentDate,
            content: 'x',
            is_deleted: false,
          },
        ],
    ));
    mockReplyRepository.getRepliesByThreadId = jest.fn(() => Promise.resolve(
        [
          {
            id: 'reply-123',
            comment_id: 'comment-123',
            content: 'some reply',
            date: currentDate,
            is_deleted: false,
            username: 'dicoding',
          },
        ],
    ));

    const getThreadByIdUseCase = new GetThreadByIdUseCase({
      threadRepository: mockThreadRepository,
      commentRepository: mockCommentRepository,
      replyRepository: mockReplyRepository,
    });

    const thread = await getThreadByIdUseCase.execute(useCasePayload.id);

    expect(thread).toStrictEqual(expectedThread);
    expect(mockThreadRepository.getThreadById)
        .toHaveBeenCalledWith(useCasePayload.id);
    expect(mockCommentRepository.getCommentsByThreadId)
        .toHaveBeenCalledWith(useCasePayload.id);
    expect(mockReplyRepository.getRepliesByThreadId)
        .toHaveBeenCalledWith(useCasePayload.id);
  });

  it('should not display deleted content', async () => {
    const useCasePayload = { id: 'thread-123' };
    const currentDate = new Date();
    const expectedThread = {
      id: 'thread-123',
      title: 'some thread',
      body: 'anything',
      date: currentDate,
      username: 'dicoding',
      comments: [
        {
          id: 'comment-123',
          username: 'dicoding',
          date: currentDate,
          content: 'x',
          replies: [
            {
              id: 'reply-123',
              content: '**balasan telah dihapus**',
              date: currentDate,
              username: 'dicoding',
            },
          ],
        },
        {
          id: 'comment-124',
          username: 'dicoding',
          date: currentDate,
          content: '**komentar telah dihapus**',
          replies: [],
        },
      ],
    };

    const mockThreadRepository = new ThreadRepository();
    const mockCommentRepository = new CommentRepository();
    const mockReplyRepository = new ReplyRepository();

    mockThreadRepository.getThreadById = jest.fn(() => Promise.resolve(
        new DetailThread({
          id: 'thread-123',
          title: 'some thread',
          body: 'anything',
          date: currentDate,
          username: 'dicoding',
        }),
    ));
    mockCommentRepository.getCommentsByThreadId = jest.fn(() => Promise.resolve(
        [
          {
            id: 'comment-123',
            username: 'dicoding',
            date: currentDate,
            content: 'x',
            is_deleted: false,
          },
          {
            id: 'comment-124',
            username: 'dicoding',
            date: currentDate,
            content: 'x',
            is_deleted: true,
          },
        ],
    ));
    mockReplyRepository.getRepliesByThreadId = jest.fn(() => Promise.resolve(
        [
          {
            id: 'reply-123',
            comment_id: 'comment-123',
            content: 'some reply',
            date: currentDate,
            is_deleted: true,
            username: 'dicoding',
          },
        ],
    ));

    const getThreadByIdUseCase = new GetThreadByIdUseCase({
      threadRepository: mockThreadRepository,
      commentRepository: mockCommentRepository,
      replyRepository: mockReplyRepository,
    });

    const thread = await getThreadByIdUseCase.execute(useCasePayload.id);

    expect(thread).toStrictEqual(expectedThread);
    expect(mockThreadRepository.getThreadById)
        .toHaveBeenCalledWith(useCasePayload.id);
    expect(mockCommentRepository.getCommentsByThreadId)
        .toHaveBeenCalledWith(useCasePayload.id);
    expect(mockReplyRepository.getRepliesByThreadId)
        .toHaveBeenCalledWith(useCasePayload.id);
  });
});

const AuthorizationError = require('../../Commons/exceptions/AuthorizationError');
const NotFoundError = require('../../Commons/exceptions/NotFoundError');
const CommentRepository = require('../../Domains/comments/CommentRepository');
const AddedComment = require('../../Domains/comments/entities/AddedComment');

class CommentRepositoryPostgres extends CommentRepository {
  constructor(pool, idGenerator) {
    super();

    this._pool = pool;
    this._idGenerator = idGenerator;
  }

  async addComment(newComment) {
    const { content, threadId, owner } = newComment;
    const id = `comment-${this._idGenerator()}`;
    const currentDate = new Date();
    const query = {
      text: `
        INSERT INTO comments VALUES($1,$2,$3,$4,$5)
        RETURNING id, content, owner
      `,
      values: [id, threadId, owner, content, currentDate],
    };

    const { rows } = await this._pool.query(query);

    return new AddedComment(rows[0]);
  }

  async verifyCommentOwner(id, owner) {
    const query = {
      text: 'SELECT 1 FROM comments WHERE id=$1 AND owner=$2',
      values: [id, owner],
    };

    const { rowCount } = await this._pool.query(query);
    if (!rowCount) {
      throw new AuthorizationError('anda tidak memiliki akses pada resource ini');
    }
  }

  async deleteCommentById(id) {
    const query = {
      text: 'UPDATE comments SET is_deleted=TRUE WHERE id=$1',
      values: [id],
    };

    const { rowCount } = await this._pool.query(query);
    if (!rowCount) {
      throw new NotFoundError('komentar tidak ditemukan');
    }
  }

  async getCommentsByThreadId(threadId) {
    const query = {
      text: `
        SELECT
          cmt.id,
          u.username,
          cmt.date,
          cmt.content,
          cmt.is_deleted
        FROM comments AS cmt
        JOIN users AS u ON u.id=cmt.owner
        WHERE cmt.thread_id=$1
        ORDER BY cmt.date
      `,
      values: [threadId],
    };

    const { rows } = await this._pool.query(query);

    return rows;
  }

  async verifyAvailableCommentInThread(id, threadId) {
    const query = {
      text: `
        SELECT 1 FROM comments
        WHERE id=$1 AND thread_id=$2 AND is_deleted=FALSE
      `,
      values: [id, threadId],
    };

    const { rowCount } = await this._pool.query(query);
    if (!rowCount) {
      throw new NotFoundError('komentar pada thread ini tidak ditemukan');
    }
  }
}

module.exports = CommentRepositoryPostgres;

const AuthorizationError = require('../../Commons/exceptions/AuthorizationError');
const NotFoundError = require('../../Commons/exceptions/NotFoundError');
const ReplyRepository = require('../../Domains/replies/ReplyRepository');
const AddedReply = require('../../Domains/replies/entities/AddedReply');

class ReplyRepositoryPostgres extends ReplyRepository {
  constructor(pool, idGenerator) {
    super();

    this._pool = pool;
    this._idGenerator = idGenerator;
  }

  async addReply({ content, commentId, owner }) {
    const id = `reply-${this._idGenerator()}`;
    const currDate = new Date();
    const query = {
      text: `
        INSERT INTO replies VALUES($1,$2,$3,$4,$5)
        RETURNING id, content, owner
      `,
      values: [id, commentId, owner, content, currDate],
    };

    const { rows } = await this._pool.query(query);

    return new AddedReply(rows[0]);
  }

  async verifyReplyOwner({ replyId, userId }) {
    const query = {
      text: 'SELECT 1 FROM replies WHERE id=$1 AND owner=$2',
      values: [replyId, userId],
    };

    const { rowCount } = await this._pool.query(query);
    if (!rowCount) {
      throw new AuthorizationError('anda tidak memiliki akses pada resource ini');
    }
  }

  async deleteReplyById(id) {
    const query = {
      text: 'UPDATE replies SET is_deleted=TRUE WHERE id=$1',
      values: [id],
    };

    const { rowCount } = await this._pool.query(query);
    if (!rowCount) {
      throw new NotFoundError('balasan tidak ditemukan');
    }
  }

  async getRepliesByThreadId(threadId) {
    const query = {
      text: `
        SELECT
          rpl.id,
          rpl.comment_id,
          rpl.content,
          rpl.date,
          rpl.is_deleted,
          u.username
        FROM replies AS rpl
        JOIN users AS u ON u.id = rpl.owner
        JOIN comments AS c ON c.id = rpl.comment_id
        WHERE c.thread_id=$1
        ORDER BY c.date, rpl.date
      `,
      values: [threadId],
    };

    const { rows } = await this._pool.query(query);

    return rows;
  }

  async verifyAvailableReply({ replyId, commentId, threadId }) {
    const query = {
      text: `
        SELECT 1
        FROM replies AS rpl
        JOIN comments AS c ON c.id=rpl.comment_id
        WHERE rpl.id=$1
        AND rpl.comment_id=$2
        AND c.thread_id=$3
        AND rpl.is_deleted=FALSE
      `,
      values: [replyId, commentId, threadId],
    };

    const { rowCount } = await this._pool.query(query);
    if (!rowCount) {
      throw new NotFoundError('balasan tidak ditemukan');
    }
  }
}

module.exports = ReplyRepositoryPostgres;

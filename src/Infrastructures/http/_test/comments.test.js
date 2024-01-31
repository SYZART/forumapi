/* eslint-disable max-len */
const pool = require('../../database/postgres/pool');
const CommentsTableTestHelper = require('../../../../tests/CommentsTableTestHelper');
const ThreadsTableTestHelper = require('../../../../tests/ThreadsTableTestHelper');
const UsersTableTestHelper = require('../../../../tests/UsersTableTestHelper');
const AuthenticationsTableTestHelper = require('../../../../tests/AuthenticationsTableTestHelper');
const container = require('../../container');
const createServer = require('../createServer');
const ServerTestHelper = require('../../../../tests/ServerTestHelper');

describe('/threads/{threadId}/comments/{commentId} endpoint', () => {
  afterAll(async () => {
    await pool.end;
  });

  afterEach(async () => {
    await CommentsTableTestHelper.cleanTable();
    await ThreadsTableTestHelper.cleanTable();
    await UsersTableTestHelper.cleanTable();
    await AuthenticationsTableTestHelper.cleanTable();
  });

  describe('when POST /threads/{threadId}/comments', () => {
    it('should response 401 when missing authentication', async () => {
      const server = await createServer(container);

      const response = await server.inject({
        method: 'POST',
        url: '/threads/thread-123/comments',
      });

      const responseJson = JSON.parse(response.payload);
      expect(response.statusCode).toEqual(401);
      expect(responseJson.error).toEqual('Unauthorized');
    });

    it('should response 400 when request payload not contain needed property', async () => {
      const server = await createServer(container);

      const { accessToken } = await ServerTestHelper
          .getAccessTokenAndUserIdHelper({ server });

      const response = await server.inject({
        method: 'POST',
        url: '/threads/thread-123/comments',
        payload: {},
        headers: {
          Authorization: `Bearer ${accessToken}`,
        },
      });

      const responseJson = JSON.parse(response.payload);
      expect(response.statusCode).toEqual(400);
      expect(responseJson.status).toEqual('fail');
      expect(responseJson.message)
          .toEqual('tidak dapat membuat komentar baru karena properti yang dibutuhkan tidak ada');
    });

    it('should response 400 when request payload not meet data type spec', async () => {
      const reqPayload = {
        content: 123,
      };

      const server = await createServer(container);

      const { accessToken } = await ServerTestHelper
          .getAccessTokenAndUserIdHelper({ server });

      const response = await server.inject({
        method: 'POST',
        url: '/threads/thread-123/comments',
        payload: reqPayload,
        headers: {
          Authorization: `Bearer ${accessToken}`,
        },
      });

      const responseJson = JSON.parse(response.payload);
      expect(response.statusCode).toEqual(400);
      expect(responseJson.status).toEqual('fail');
      expect(responseJson.message)
          .toEqual('tidak dapat membuat komentar baru karena tipe data tidak sesuai');
    });

    it('should response 404 when thread is not found', async () => {
      const reqPayload = {
        content: 'x',
      };

      const server = await createServer(container);

      const { accessToken } = await ServerTestHelper
          .getAccessTokenAndUserIdHelper({ server });

      const response = await server.inject({
        method: 'POST',
        url: '/threads/thread-123/comments',
        payload: reqPayload,
        headers: {
          Authorization: `Bearer ${accessToken}`,
        },
      });

      const responseJson = JSON.parse(response.payload);
      expect(response.statusCode).toEqual(404);
      expect(responseJson.status).toEqual('fail');
      expect(responseJson.message).toEqual('thread tidak ditemukan');
    });

    it('should response 201 and persisted comment', async () => {
      await UsersTableTestHelper.addUser({});
      await ThreadsTableTestHelper.addThread({});

      const reqPayload = {
        content: 'x',
      };

      const server = await createServer(container);

      const { accessToken } = await ServerTestHelper
          .getAccessTokenAndUserIdHelper({ server, username: 'x' });

      const response = await server.inject({
        method: 'POST',
        url: '/threads/thread-123/comments',
        payload: reqPayload,
        headers: {
          Authorization: `Bearer ${accessToken}`,
        },
      });

      const responseJson = JSON.parse(response.payload);
      expect(response.statusCode).toEqual(201);
      expect(responseJson.status).toEqual('success');
      expect(responseJson.data.addedComment).toBeDefined();
      expect(responseJson.data.addedComment.id).toBeDefined();
      expect(responseJson.data.addedComment.content).toEqual(reqPayload.content);
      expect(responseJson.data.addedComment.owner).toBeDefined();
    });
  });

  describe('when DELETE /threads/{threadId}/comments/{commentId}', () => {
    it('should response 401 when missing authentication', async () => {
      const server = await createServer(container);

      const response = await server.inject({
        method: 'DELETE',
        url: '/threads/thread-123/comments/comment-123',
      });

      const responseJson = JSON.parse(response.payload);
      expect(response.statusCode).toEqual(401);
      expect(responseJson.error).toEqual('Unauthorized');
    });

    it('should response 404 when thread is not found', async () => {
      const server = await createServer(container);
      const { accessToken } = await ServerTestHelper
          .getAccessTokenAndUserIdHelper({ server });

      const response = await server.inject({
        method: 'DELETE',
        url: '/threads/thread-123/comments/comment-123',
        headers: {
          Authorization: `Bearer ${accessToken}`,
        },
      });

      const responseJson = JSON.parse(response.payload);
      expect(response.statusCode).toEqual(404);
      expect(responseJson.status).toEqual('fail');
      expect(responseJson.message).toEqual('komentar pada thread ini tidak ditemukan');
    });

    it('should response 404 when comment is not found', async () => {
      const server = await createServer(container);

      const { userId, accessToken } = await ServerTestHelper
          .getAccessTokenAndUserIdHelper({ server });

      await ThreadsTableTestHelper.addThread({ owner: userId });

      const response = await server.inject({
        method: 'DELETE',
        url: '/threads/thread-123/comments/comment-123',
        headers: {
          Authorization: `Bearer ${accessToken}`,
        },
      });

      const responseJson = JSON.parse(response.payload);
      expect(response.statusCode).toEqual(404);
      expect(responseJson.status).toEqual('fail');
      expect(responseJson.message).toEqual('komentar pada thread ini tidak ditemukan');
    });

    it('should response 403 when user is not the owner', async () => {
      await UsersTableTestHelper.addUser({ username: 'user' });
      await ThreadsTableTestHelper.addThread({});
      await CommentsTableTestHelper.addComment({});

      const server = await createServer(container);
      const { accessToken } = await ServerTestHelper
          .getAccessTokenAndUserIdHelper({ server });

      const response = await server.inject({
        method: 'DELETE',
        url: '/threads/thread-123/comments/comment-123',
        headers: {
          Authorization: `Bearer ${accessToken}`,
        },
      });

      const responseJson = JSON.parse(response.payload);
      expect(response.statusCode).toEqual(403);
      expect(responseJson.status).toEqual('fail');
      expect(responseJson.message)
          .toEqual('anda tidak memiliki akses pada resource ini');
    });

    it('should response 200 and delete comment', async () => {
      const server = await createServer(container);
      const { userId, accessToken } = await ServerTestHelper
          .getAccessTokenAndUserIdHelper({ server, username: 'x' });

      await ThreadsTableTestHelper.addThread({ owner: userId });
      await CommentsTableTestHelper.addComment({ owner: userId });

      const response = await server.inject({
        method: 'DELETE',
        url: '/threads/thread-123/comments/comment-123',
        headers: {
          Authorization: `Bearer ${accessToken}`,
        },
      });

      const responseJson = JSON.parse(response.payload);
      expect(response.statusCode).toEqual(200);
      expect(responseJson.status).toEqual('success');
    });
  });
});

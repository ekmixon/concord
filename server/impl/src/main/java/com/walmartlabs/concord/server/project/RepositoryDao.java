package com.walmartlabs.concord.server.project;

import com.walmartlabs.concord.common.db.AbstractDao;
import com.walmartlabs.concord.server.api.IdName;
import com.walmartlabs.concord.server.api.project.RepositoryEntry;
import com.walmartlabs.concord.server.user.UserPermissionCleaner;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;

import static com.walmartlabs.concord.server.jooq.public_.tables.Repositories.REPOSITORIES;
import static com.walmartlabs.concord.server.jooq.public_.tables.Secrets.SECRETS;

@Named
public class RepositoryDao extends AbstractDao {

    private static final Logger log = LoggerFactory.getLogger(RepositoryDao.class);

    private final UserPermissionCleaner permissionCleaner;

    @Inject
    public RepositoryDao(Configuration cfg, UserPermissionCleaner permissionCleaner) {
        super(cfg);
        this.permissionCleaner = permissionCleaner;
    }

    public boolean exists(String projectId, String repositoryName) {
        try (DSLContext create = DSL.using(cfg)) {
            return create.fetchExists(create.selectFrom(REPOSITORIES)
                    .where(REPOSITORIES.PROJECT_ID.eq(projectId)
                            .and(REPOSITORIES.REPO_NAME.eq(repositoryName))));
        }
    }

    public RepositoryEntry getByNameInProject(String projectId, String repositoryName) {
        try (DSLContext create = DSL.using(cfg)) {
            return selectRepositoryEntry(create)
                    .where(REPOSITORIES.PROJECT_ID.eq(projectId)
                            .and(REPOSITORIES.REPO_NAME.eq(repositoryName)))
                    .fetchOne(RepositoryDao::toEntry);
        }
    }

    public void insert(String projectId, String repositoryName, String url, String branch, String secretId) {
        tx(tx -> {
            insert(tx, projectId, repositoryName, url, branch, secretId);
        });
    }

    public void insert(DSLContext create, String projectId, String repositoryName, String url, String branch, String secretId) {
        create.insertInto(REPOSITORIES)
                .columns(REPOSITORIES.PROJECT_ID, REPOSITORIES.REPO_NAME,
                        REPOSITORIES.REPO_URL, REPOSITORIES.REPO_BRANCH, REPOSITORIES.SECRET_ID)
                .values(projectId, repositoryName, url, branch, secretId)
                .execute();
    }

    public void update(DSLContext create, String repositoryName, String url, String branch, String secretId) {
        create.update(REPOSITORIES)
                .set(REPOSITORIES.REPO_URL, url)
                .set(REPOSITORIES.SECRET_ID, secretId)
                .set(REPOSITORIES.REPO_BRANCH, branch)
                .where(REPOSITORIES.REPO_NAME.eq(repositoryName))
                .execute();
    }

    public void delete(DSLContext create, String repositoryName) {
        permissionCleaner.onRepositoryRemoval(create, repositoryName);
        create.deleteFrom(REPOSITORIES)
                .where(REPOSITORIES.REPO_NAME.eq(repositoryName))
                .execute();
    }

    public List<RepositoryEntry> list(String projectId, Field<?> sortField, boolean asc) {
        try (DSLContext create = DSL.using(cfg)) {
            SelectConditionStep<Record5<String, String, String, String, String>> query = selectRepositoryEntry(create)
                    .where(REPOSITORIES.PROJECT_ID.eq(projectId));

            if (sortField != null) {
                query.orderBy(asc ? sortField.asc() : sortField.desc());
            }

            List<RepositoryEntry> result = query.fetch(RepositoryDao::toEntry);
            log.info("list [{}, {}] -> got {} result(s)", result.size());
            return result;
        }
    }

    private static SelectJoinStep<Record5<String, String, String, String, String>> selectRepositoryEntry(DSLContext create) {
        return create.select(REPOSITORIES.REPO_NAME,
                REPOSITORIES.REPO_URL,
                REPOSITORIES.REPO_BRANCH,
                SECRETS.SECRET_ID,
                SECRETS.SECRET_NAME)
                .from(REPOSITORIES)
                .leftOuterJoin(SECRETS).on(SECRETS.SECRET_ID.eq(REPOSITORIES.SECRET_ID));
    }

    private static RepositoryEntry toEntry(Record5<String, String, String, String, String> r) {
        String secretId = r.get(SECRETS.SECRET_ID);
        IdName secret = secretId != null ? new IdName(secretId, r.get(SECRETS.SECRET_NAME)) : null;

        return new RepositoryEntry(r.get(REPOSITORIES.REPO_NAME),
                r.get(REPOSITORIES.REPO_URL),
                r.get(REPOSITORIES.REPO_BRANCH),
                secret);
    }
}

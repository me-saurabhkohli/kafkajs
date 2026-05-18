const Encoder = require('../../../protocol/encoder')
const Decoder = require('../../../protocol/decoder')
const { MemberMetadata, MemberAssignment } = require('../../assignerProtocol')

const DEFAULT_GENERATION = -1
const CONTESTED = 'CONTESTED'

const encodeUserData = (generationId, ownedPartitions) =>
  new Encoder()
    .writeInt32(generationId)
    .writeArray(
      Object.entries(ownedPartitions).map(([topic, partitions]) =>
        new Encoder().writeString(topic).writeArray(partitions)
      )
    ).buffer

const decodeUserData = buffer => {
  if (!buffer || buffer.length === 0) {
    return { generationId: DEFAULT_GENERATION, ownedPartitions: {} }
  }

  try {
    const decoder = new Decoder(buffer)
    const generationId = decoder.readInt32()
    const entries = decoder.readArray(d => ({
      topic: d.readString(),
      partitions: d.readArray(pd => pd.readInt32()),
    }))
    const ownedPartitions = entries.reduce(
      (acc, { topic, partitions }) => ({ ...acc, [topic]: partitions }),
      {}
    )

    return { generationId, ownedPartitions }
  } catch (_) {
    return { generationId: DEFAULT_GENERATION, ownedPartitions: {} }
  }
}

const safeDecodeMemberMetadata = (memberMetadata, allTopics, version) => {
  if (!memberMetadata) {
    return {
      version,
      topics: allTopics,
      userData: Buffer.alloc(0),
    }
  }

  try {
    return MemberMetadata.decode(memberMetadata)
  } catch (_) {
    return {
      version,
      topics: allTopics,
      userData: Buffer.alloc(0),
    }
  }
}

/**
 * CooperativeStickyAssigner
 *
 * Sticky + cooperative assignment strategy (KIP-429 style):
 *  1) Keep current ownership where possible (sticky)
 *  2) Balance to quotas
 *  3) If a partition transfer is needed, withhold it from the new owner this round
 *     so the previous owner can revoke first (cooperative two-step handoff)
 */
module.exports = ({ cluster }) => {
  let currentGenerationId = DEFAULT_GENERATION
  let currentOwnedPartitions = {}

  return {
    name: 'cooperative-sticky',
    version: 1,

    onAssignment({ assignment, generationId }) {
      const hadRevocations =
        currentGenerationId !== DEFAULT_GENERATION &&
        Object.entries(currentOwnedPartitions).some(([topic, parts]) =>
          parts.some(p => !(assignment[topic] || []).includes(p))
        )

      currentGenerationId = generationId
      currentOwnedPartitions = assignment
      return hadRevocations
    },

    async assign({ members, topics }) {
      const sortedMembers = members.map(m => m.memberId).sort()
      const memberInfo = {}

      for (const { memberId, memberMetadata } of members) {
        const decoded = safeDecodeMemberMetadata(memberMetadata, topics, this.version)
        const { generationId, ownedPartitions } = decodeUserData(decoded.userData)

        memberInfo[memberId] = {
          subscribedTopics: decoded.topics && decoded.topics.length > 0 ? decoded.topics : topics,
          generationId,
          ownedPartitions,
        }
      }

      const topicPartitions = {}
      for (const topic of topics) {
        topicPartitions[topic] = cluster
          .findTopicPartitionMetadata(topic)
          .map(m => m.partitionId)
          .sort((a, b) => a - b)
      }

      const prevOwner = {}
      for (const topic of topics) {
        prevOwner[topic] = {}
      }

      for (const memberId of sortedMembers) {
        const { ownedPartitions, generationId, subscribedTopics } = memberInfo[memberId]

        for (const [topic, partitions] of Object.entries(ownedPartitions)) {
          if (!topicPartitions[topic]) continue
          if (!subscribedTopics.includes(topic)) continue

          for (const partitionId of partitions) {
            if (!topicPartitions[topic].includes(partitionId)) continue

            const existing = prevOwner[topic][partitionId]
            if (existing === undefined) {
              prevOwner[topic][partitionId] = memberId
            } else if (existing !== CONTESTED) {
              const existingGen = memberInfo[existing].generationId
              if (existingGen === generationId) {
                prevOwner[topic][partitionId] = CONTESTED
              } else if (generationId > existingGen) {
                prevOwner[topic][partitionId] = memberId
              }
            }
          }
        }
      }

      const assignment = {}
      for (const memberId of sortedMembers) {
        assignment[memberId] = {}
      }

      const unassigned = []
      for (const topic of topics) {
        for (const partitionId of topicPartitions[topic]) {
          const owner = prevOwner[topic][partitionId]
          if (
            owner &&
            owner !== CONTESTED &&
            assignment[owner] !== undefined &&
            memberInfo[owner].subscribedTopics.includes(topic)
          ) {
            if (!assignment[owner][topic]) assignment[owner][topic] = []
            assignment[owner][topic].push(partitionId)
          } else {
            unassigned.push({ topic, partitionId })
          }
        }
      }

      const totalPartitions = topics.reduce((sum, topic) => sum + topicPartitions[topic].length, 0)
      const memberCount = sortedMembers.length
      const minQuota = Math.floor(totalPartitions / memberCount)
      const maxQuota = Math.ceil(totalPartitions / memberCount)
      const expectedMaxQuotaMembers = totalPartitions % memberCount

      const countPartitions = memberId =>
        Object.values(assignment[memberId]).reduce((sum, parts) => sum + parts.length, 0)

      const trimTo = (memberId, target) => {
        while (countPartitions(memberId) > target) {
          let revoked = false
          for (const topic of Object.keys(assignment[memberId])) {
            if (assignment[memberId][topic] && assignment[memberId][topic].length > 0) {
              const partitionId = assignment[memberId][topic].pop()
              unassigned.push({ topic, partitionId })
              if (assignment[memberId][topic].length === 0) {
                delete assignment[memberId][topic]
              }
              revoked = true
              break
            }
          }
          if (!revoked) break
        }
      }

      let currentMaxQuotaMembers = 0
      for (const memberId of sortedMembers) {
        trimTo(memberId, maxQuota)

        if (countPartitions(memberId) > minQuota) {
          if (currentMaxQuotaMembers < expectedMaxQuotaMembers) {
            currentMaxQuotaMembers++
          } else {
            trimTo(memberId, minQuota)
          }
        }
      }

      let memberIdx = 0
      for (const { topic, partitionId } of unassigned) {
        let placed = false

        for (let i = 0; i < sortedMembers.length; i++) {
          const idx = (memberIdx + i) % sortedMembers.length
          const candidate = sortedMembers[idx]
          if (
            memberInfo[candidate].subscribedTopics.includes(topic) &&
            countPartitions(candidate) < minQuota
          ) {
            if (!assignment[candidate][topic]) assignment[candidate][topic] = []
            assignment[candidate][topic].push(partitionId)
            memberIdx = (idx + 1) % sortedMembers.length
            placed = true
            break
          }
        }

        if (!placed) {
          for (let i = 0; i < sortedMembers.length; i++) {
            const idx = (memberIdx + i) % sortedMembers.length
            const candidate = sortedMembers[idx]
            if (
              memberInfo[candidate].subscribedTopics.includes(topic) &&
              countPartitions(candidate) < maxQuota
            ) {
              if (!assignment[candidate][topic]) assignment[candidate][topic] = []
              assignment[candidate][topic].push(partitionId)
              memberIdx = (idx + 1) % sortedMembers.length
              placed = true
              break
            }
          }
        }
      }

      const makeKeySet = owned => {
        const keys = new Set()
        for (const [topic, parts] of Object.entries(owned)) {
          for (const p of parts) keys.add(`${topic}:${p}`)
        }
        return keys
      }

      const allAdded = {}
      const allRevoked = new Set()
      for (const memberId of sortedMembers) {
        const ownedSet = makeKeySet(memberInfo[memberId].ownedPartitions)
        const assignedSet = makeKeySet(assignment[memberId])

        for (const key of assignedSet) {
          if (!ownedSet.has(key)) allAdded[key] = memberId
        }
        for (const key of ownedSet) {
          if (!assignedSet.has(key)) allRevoked.add(key)
        }
      }

      for (const [key, newOwner] of Object.entries(allAdded)) {
        if (!allRevoked.has(key)) continue

        const split = key.indexOf(':')
        const topic = key.slice(0, split)
        const partitionId = parseInt(key.slice(split + 1), 10)

        if (assignment[newOwner][topic]) {
          const idx = assignment[newOwner][topic].indexOf(partitionId)
          if (idx !== -1) {
            assignment[newOwner][topic].splice(idx, 1)
            if (assignment[newOwner][topic].length === 0) {
              delete assignment[newOwner][topic]
            }
          }
        }
      }

      return sortedMembers.map(memberId => ({
        memberId,
        memberAssignment: MemberAssignment.encode({
          version: this.version,
          assignment: assignment[memberId],
        }),
      }))
    },

    protocol({ topics }) {
      return {
        name: this.name,
        metadata: MemberMetadata.encode({
          version: this.version,
          topics,
          userData: encodeUserData(currentGenerationId, currentOwnedPartitions),
        }),
      }
    },
  }
}

module.exports.encodeUserData = encodeUserData
module.exports.decodeUserData = decodeUserData
